var fs = require("fs");
var { Transaction, cry } = require("@meterio/devkit");
var redis = require("redis");
var async = require("async");
var util = require("meter-stratum-pool/lib/util.js");
var meterify = require("meterify").meterify;
var Web3 = require("web3");
const { default: BigNumber } = require("bignumber.js");

const ADDR_PATTERN = RegExp("^0x[0-9a-fA-F]{40}$");

module.exports = function (logger) {
  var poolConfigs = JSON.parse(process.env.pools);

  var enabledPools = [];

  Object.keys(poolConfigs).forEach(function (coin) {
    var poolOptions = poolConfigs[coin];
    if (poolOptions.rewardProcessing && poolOptions.rewardProcessing.enabled)
      enabledPools.push(coin);
  });
  console.log("enabledpools: ", enabledPools);

  async.filter(
    enabledPools,
    function (coin, callback) {
      SetupForPool(logger, poolConfigs[coin], function (setupResults) {
        console.log("setup resutl: ", setupResults);
        callback(null, setupResults);
      });
    },
    function (err, coins) {
      console.log("coins", coins);
      coins.forEach(function (coin) {
        var poolOptions = poolConfigs[coin];
        var processingConfig = poolOptions.rewardProcessing;
        var logSystem = "Rewards";
        var logComponent = coin;

        logger.debug(
          logSystem,
          logComponent,
          "Reward processing setup to run every " +
            processingConfig.rewardInterval +
            " second(s) with daemon (" +
            processingConfig.restfulURL +
            ") and redis (" +
            poolOptions.redis.host +
            ":" +
            poolOptions.redis.port +
            ")"
        );
      });
    }
  );
};

function SetupForPool(logger, poolOptions, setupFinished) {
  var coin = poolOptions.coin.name;
  var processingConfig = poolOptions.rewardProcessing;

  const content = fs.readFileSync(processingConfig.keystoreFile);
  const passphrase = processingConfig.passphrase;
  const j = JSON.parse(content);

  var logSystem = "Rewards";
  var logComponent = coin;
  var beneficiary = "0x" + poolOptions.rewardBeneficiary.toLowerCase();

  logger.debug(
    logSystem,
    logComponent,
    "restfulURL: " + processingConfig.restfulURL
  );
  logger.debug(logSystem, logComponent, "beneficiary: " + beneficiary);
  var web3 = meterify(new Web3(), processingConfig.restfulURL);
  var poolPK = "0x";

  var redisClient = redis.createClient(
    poolOptions.redis.port,
    poolOptions.redis.host
  );

  var poolTax = Number(processingConfig.poolTax);
  var poolTaxReceiver = processingConfig.poolTaxReceiver.toLowerCase();
  var minimumPayment = new BigNumber(processingConfig.minimumPayment);

  var rewardInterval;

  async.parallel(
    [
      function (callback) {
        cry.Keystore.decrypt(j, passphrase).then(function (encrypted) {
          poolPK = "0x" + encrypted.toString("hex");
          web3.eth.accounts.wallet.add(poolPK);
          callback(null);
        });
      },
      function (callback) {
        web3.eth.getEnergy(beneficiary, function (err, balance) {
          logger.debug(logSystem, logComponent, "balance: " + err + balance);
          callback(null);
        });
      },
    ],
    function (err) {
      console.log("cb: err: ", err);
      if (err) {
        console.log("setup finished", false);
        setupFinished(false);
        return;
      }
      if (poolTax < 0 || poolTax > 100) {
        console.log("poolTax out of range[0,100]");
        setupFinished(false);
        return;
      }

      if (!poolTaxReceiver.match(ADDR_PATTERN)) {
        console.log("poolTaxReceiver is invalid address");
        setupFinished(false);
        return;
      }
      if (Number.isNaN(minimumPayment.toNumber())) {
        console.log("minimumPayment is invalid");
        setupFinished(false);
        return;
      }
      rewardInterval = setInterval(function () {
        try {
          processRewards();
        } catch (e) {
          throw e;
        }
      }, processingConfig.rewardInterval * 1000);
      setTimeout(processRewards, 100);
      console.log("setup finished", true);
      setupFinished(true);
    }
  );

  /* Deal with numbers in smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */

  var processRewards = function () {
    var startRewardProcess = Date.now();

    var timeSpentRPC = 0;
    var timeSpentRedis = 0;

    var startTimeRedis;
    var startTimeRPC;

    var startRedisTimer = function () {
      startTimeRedis = Date.now();
    };
    var endRedisTimer = function () {
      timeSpentRedis += Date.now() - startTimeRedis;
    };

    var startRPCTimer = function () {
      startTimeRPC = Date.now();
    };
    var endRPCTimer = function () {
      timeSpentRPC += Date.now() - startTimeRedis;
    };

    async.waterfall(
      [
        /* Call redis to get an array of pending rounds - which are coinbase transactions and block heights from submitted blocks. */
        function (callback) {
          startRedisTimer();
          logger.debug(
            logSystem,
            logComponent,
            "Call redis to get balances and rounds"
          );
          redisClient
            .multi([
              ["hgetall", coin + ":balances"],
              ["smembers", coin + ":blocksPending"],
            ])
            .exec(function (error, results) {
              endRedisTimer();

              if (error) {
                logger.error(
                  logSystem,
                  logComponent,
                  "Could not get blocks from redis " + JSON.stringify(error)
                );
                callback(true);
                return;
              }

              // calculate worker balances
              console.log("workers: ", results[0]);
              var workers = {};
              for (var w in results[0]) {
                workers[w] = {
                  balance: parseFloat(results[0][w]),
                };
              }

              // if no pending rounds, stop right here
              if (results[1].length <= 0) {
                logger.debug(
                  logSystem,
                  logComponent,
                  "No pending blocks to process, skip"
                );
                return callback(new Error("no pending blocks to calculate"));
              }

              // list out all the rounds
              var rounds = results[1].map(function (r) {
                var details = r.split(":");
                return {
                  blockHash: details[0],
                  txHash: details[1],
                  height: details[2],
                  serialized: r,
                  category: "generate",
                };
              });

              callback(null, workers, rounds);
            });
        },

        /* Does a batch redis call to get shares contributed to each round. Then calculates the totalShares
                & pendingShares for each worker.  */
        function (workers, rounds, callback) {
          var shareLookups = rounds.map(function (r) {
            return ["hgetall", coin + ":shares:round" + r.height];
          });

          let pendingShares = {}; // worker name -> shares sum for this worker
          let totalShares = 0; // total share of pending rounds

          logger.debug(
            logSystem,
            logComponent,
            "Start calculate totalShares & pendingShares"
          );
          startRedisTimer();
          redisClient
            .multi(shareLookups)
            .exec(function (error, allWorkerShares) {
              endRedisTimer();

              if (error) {
                return callback(
                  error,
                  "Check finished - redis error with multi get rounds share"
                );
              }

              rounds.forEach(function (round, i) {
                var workerShares = allWorkerShares[i];

                if (!workerShares) {
                  logger.error(
                    logSystem,
                    logComponent,
                    "No worker shares for round: " +
                      round.height +
                      " blockHash: " +
                      round.blockHash
                  );
                  return;
                }

                switch (round.category) {
                  case "generate":
                    /* We found a confirmed block! Now get the reward for it and calculate how much
                                   we owe each miner based on the shares they submitted during that block round. */

                    var totalSharesInRound = Object.keys(workerShares).reduce(
                      function (p, c) {
                        if (c != "time") {
                          return p + parseFloat(workerShares[c]);
                        }
                        return p;
                      },
                      0
                    );

                    totalShares += totalSharesInRound;
                    logger.debug(
                      logSystem,
                      logComponent,
                      `Total shares in round ${round.height}: ${totalSharesInRound}`
                    );

                    for (var workerAddress in workerShares) {
                      if (!(workerAddress in pendingShares)) {
                        pendingShares[workerAddress] = 0;
                      }
                      if (workerAddress == "time") {
                        continue;
                      }
                      pendingShares[workerAddress] += Number(
                        workerShares[workerAddress]
                      );
                    }
                    break;
                }
              });

              logger.debug(
                logSystem,
                logComponent,
                `Total Shares: ${totalShares}, len(PendingShares): ${
                  Object.keys(pendingShares).length
                }`
              );

              callback(null, workers, rounds, totalShares, pendingShares);
            });
        },

        /**
         *  Load balance/reserve on pool account and calculate actual reward amount for each miner
         *  according to their shares
         */
        function (workers, rounds, totalShares, pendingShares, callback) {
          logger.debug(
            logSystem,
            logComponent,
            `Read balance/reserve and calculate the actual reward for each worker`
          );
          startRPCTimer();
          web3.eth.getEnergy(beneficiary, function (err, bal) {
            endRPCTimer();
            let poolBalance = new BigNumber(bal);
            if (err) {
              logger.warning(
                logSystem,
                logComponent,
                "web3.eth.getEnergy error: " + err
              );
              return callback(err);
            }
            if (Number.isNaN(Number(bal))) {
              return callback(new Error("could not load balance"));
            }

            // Calculate actual total reward
            const txFee = new BigNumber(
              Object.keys(pendingShares).length * 16000 + 5000
            ).times(500e9);
            let poolReserve = new BigNumber(0);
            if (beneficiary in workers) {
              poolReserve = new BigNumber(workers[beneficiary].balance || 0);
            }
            const actualTotalReward = poolBalance
              .minus(poolReserve)
              .minus(txFee);
            logger.debug(
              logSystem,
              logComponent,
              `Pool Balance: ${poolBalance.toFixed(
                0
              )}, Reserve: ${poolReserve.toFixed(0)}, TxFee: ${txFee.toFixed(
                0
              )}, Actual TotalReward: ${actualTotalReward.toFixed(0)}`
            );

            // if actual total reward is not enough for distribute, skip
            if (actualTotalReward.isLessThanOrEqualTo(0)) {
              logger.debug(logSystem, logComponent, "No reward this interval");
              return callback(new Error("no reward this interval"));
            }

            let leftover = new BigNumber(actualTotalReward);

            // add beneficiary
            if (!(beneficiary in workers)) {
              workers[beneficiary] = {
                balance: new BigNumber(0),
                reward: new BigNumber(0),
                issue: false,
              };
            }

            // add poolTaxReciever
            if (!(poolTaxReceiver in workers)) {
              workers[poolTaxReceiver] = {
                balance: new BigNumber(0),
                reward: new BigNumber(0),
              };
            }

            // calculate rewards for each miner
            // and accumulated pool tax to poolTaxReceiver
            for (let w in pendingShares) {
              const share = new BigNumber(pendingShares[w]).toNumber();
              w = w.toLowerCase();
              if (!(w in workers)) {
                workers[w] = {
                  reward: new BigNumber(0),
                  balance: new BigNumber(0),
                };
              } else {
                if (workers[w].reward) {
                  workers[w].reward = new BigNumber(workers[w].reward);
                } else {
                  workers[w].reward = new BigNumber(0);
                }
                if (workers[w].balance) {
                  workers[w].balance = new BigNumber(workers[w].balance);
                } else {
                  workers[w].balance = new BigNumber(0);
                }
              }

              const workerReward = actualTotalReward
                .times(share)
                .div(totalShares)
                .times(100 - poolTax)
                .div(100);
              const tax = actualTotalReward
                .times(share)
                .div(totalShares)
                .times(poolTax)
                .div(100);
              logger.debug(
                logSystem,
                logComponent,
                `worker ${w} reward: ${share}/${totalShares} * ${actualTotalReward.toFixed(
                  0
                )} * ${
                  100 - processingConfig.poolTax
                }% = ${workerReward.toFixed(0)}`
              );

              workers[poolTaxReceiver].reward = tax.plus(
                workers[poolTaxReceiver].reward
              );
              if (w.match(ADDR_PATTERN)) {
                workers[w].reward = workerReward.plus(workers[w].reward);
              } else {
                workers[w].balance = workerReward.plus(workers[w].balance);
                workers[w].issue = false;
              }
              console.log(
                `calced worker ${w}, balance: ${workers[w].balance}, reward: ${workers[w].reward}`
              );
            }

            // filter out actual payees with amount over minimum threshold
            // if amount < minimum threshold, add them to balance and payout next round
            for (w in workers) {
              if (workers[w].issue !== false) {
                const amount = workers[w].reward.plus(workers[w].balance);
                if (amount.isGreaterThan(minimumPayment)) {
                  workers[w].issue = true;
                  leftover = leftover
                    .minus(workers[w].reward)
                    .minus(workers[w].balance);
                } else {
                  workers[w].balance = workers[w].balance.plus(
                    workers[w].reward
                  );
                  workers[w].reward = new BigNumber(0);
                  workers[w].issue = false;
                }
              }
            }

            // ideally there should be no leftovers, but if it does, add it to reserve
            if (leftover.isGreaterThan(0)) {
              console.log(`add leftover to reserve: `, leftover.toFixed(0));
              workers[beneficiary].balance = new BigNumber(
                leftover.plus(workers[beneficiary]).balance.toFixed(0)
              );
              logger.debug(
                logSystem,
                logComponent,
                `Add reserve ${leftover.toFixed(0)}, totalReserve: ${workers[
                  beneficiary
                ].balance.toFixed(0)}`
              );
            }

            callback(null, workers, rounds);
          });
        },
        /* Issue a rewarding tx */
        function (workers, rounds, callback) {
          var trySend = function () {
            var addressAmounts = {};
            for (var w in workers) {
              var worker = workers[w];
              worker.balance = worker.balance || 0;
              worker.reward = worker.reward || 0;
              if (worker.issue) {
                addressAmounts[w] = new BigNumber(0)
                  .plus(worker.balance)
                  .plus(worker.reward);
              }
            }

            if (Object.keys(addressAmounts).length === 0) {
              callback(null, workers, rounds);
              return;
            }

            logger.debug(logSystem, logComponent, "Prepare to send reward tx");
            startRPCTimer();
            web3.eth.getBlockNumber(function (err, blockNum) {
              endRPCTimer();
              if (err) {
                console.log("getBlockNumber erro: ", err);
                return callback(err);
              }
              startRPCTimer();
              web3.eth.getBlock(blockNum, function (err, best) {
                endRPCTimer();
                if (err) {
                  console.log("getBlock erro: ", err);
                  return callback(err);
                }
                const blockRef = best.id.substr(0, 18);
                let chainTag = processingConfig.chainTag; // chainTag for testnet

                let dataGas = 0;
                let clauses = [];
                for (const addr in addressAmounts) {
                  const amount = addressAmounts[addr];
                  console.log(`pay to ${addr} with ${amount.toFixed(0)}`);
                  clauses.push({
                    to: addr,
                    value: amount.toFixed(0),
                    token: 0,
                    data: "0x",
                  });
                }
                const baseGas = 5000 + clauses.length * 16000; // fixed value
                let txObj = {
                  chainTag,
                  blockRef, // the first 8 bytes of latest block
                  expiration: 64, // blockRefHeight + expiration is the height for tx expire
                  clauses,
                  gasPriceCoef: 0,
                  gas: baseGas + dataGas,
                  dependsOn: null,
                  nonce: 0, // random number
                };
                let tx = new Transaction(txObj);
                const pkBuffer = Buffer.from(poolPK.replace("0x", ""), "hex");
                const signingHash = cry.blake2b256(tx.encode());
                logger.debug(logSystem, logComponent, "Signed reward tx");
                tx.signature = cry.secp256k1.sign(signingHash, pkBuffer);

                const raw = tx.encode();
                const rawTx = "0x" + raw.toString("hex");
                logger.debug(logSystem, logComponent, "Sending out reward tx");
                startRPCTimer();
                web3.eth.sendSignedTransaction(rawTx, function (err) {
                  endRPCTimer();
                  if (err) {
                    console.log("sendSignedTransaction error:", err);
                    return callback(err);
                  }
                  callback(null, workers, rounds);
                });
              });
            });
          };
          trySend(0);
        },
        function (workers, rounds, callback) {
          var totalPaid = new BigNumber(0);

          var balanceUpdateCommands = [];
          var workerPayoutsCommand = [];

          for (var w in workers) {
            var worker = workers[w];
            if (worker.issue) {
              console.log(`set ${w} balance to 0`);
              balanceUpdateCommands.push(["hset", coin + ":balances", w, 0]);
              workerPayoutsCommand.push([
                "hincrbyfloat",
                coin + ":payouts",
                w,
                new BigNumber(0)
                  .plus(worker.balance)
                  .plus(worker.reward)
                  .toNumber(),
              ]);
              totalPaid = totalPaid.plus(worker.balance).plus(worker.balance);
            } else {
              console.log(`set ${w} balance to ${worker.balance}`);
              balanceUpdateCommands.push([
                "hset",
                coin + ":balances",
                w,
                worker.balance.toFixed(0),
              ]);
            }
          }

          var movePendingCommands = [];
          var roundsToDelete = [];
          var orphanMergeCommands = [];

          var moveSharesToCurrent = function (r) {
            var workerShares = r.workerShares;
            Object.keys(workerShares).forEach(function (worker) {
              orphanMergeCommands.push([
                "hincrby",
                coin + ":shares:roundCurrent",
                worker,
                workerShares[worker],
              ]);
            });
          };

          rounds.forEach(function (r) {
            switch (r.category) {
              case "generate":
                movePendingCommands.push([
                  "smove",
                  coin + ":blocksPending",
                  coin + ":blocksConfirmed",
                  r.serialized,
                ]);
                roundsToDelete.push(coin + ":shares:round" + r.height);
                return;
            }
          });

          var finalRedisCommands = [];

          if (movePendingCommands.length > 0)
            finalRedisCommands = finalRedisCommands.concat(movePendingCommands);

          if (orphanMergeCommands.length > 0)
            finalRedisCommands = finalRedisCommands.concat(orphanMergeCommands);

          if (balanceUpdateCommands.length > 0)
            finalRedisCommands = finalRedisCommands.concat(
              balanceUpdateCommands
            );

          if (workerPayoutsCommand.length > 0)
            finalRedisCommands =
              finalRedisCommands.concat(workerPayoutsCommand);

          if (roundsToDelete.length > 0)
            finalRedisCommands.push(["del"].concat(roundsToDelete));

          if (totalPaid !== 0) {
            console.log("total paid: ", totalPaid);
            finalRedisCommands.push([
              "hincrbyfloat",
              coin + ":stats",
              "totalPaid",
              totalPaid.toFixed(0),
            ]);
          }

          if (finalRedisCommands.length === 0) {
            callback();
            return;
          }

          startRedisTimer();
          redisClient.multi(finalRedisCommands).exec(function (error, results) {
            endRedisTimer();
            if (error) {
              clearInterval(rewardInterval);
              logger.error(
                logSystem,
                logComponent,
                "Rewards sent but could not update redis. " +
                  JSON.stringify(error) +
                  " Disabling reward processing to prevent possible double-payouts. The redis commands in " +
                  coin +
                  "_finalRedisCommands.txt must be ran manually"
              );
              fs.writeFile(
                coin + "_finalRedisCommands.txt",
                JSON.stringify(finalRedisCommands),
                function (err) {
                  logger.error(
                    "Could not write finalRedisCommands.txt, you are fucked."
                  );
                }
              );
            }
            callback();
          });
        },
      ],
      function () {
        var rewardProcessTime = Date.now() - startRewardProcess;
        logger.debug(
          logSystem,
          logComponent,
          "Finished interval - time spent: " +
            rewardProcessTime +
            "ms total, " +
            timeSpentRedis +
            "ms redis, " +
            timeSpentRPC +
            "ms daemon RPC"
        );
      }
    );
  };

  var getProperAddress = function (address) {
    if (address.length === 40) {
      return util.addressFromEx(poolOptions.address, address);
    } else return address;
  };
}
