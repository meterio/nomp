var fs = require("fs");
var { Transaction, cry } = require("@meterio/devkit");
var redis = require("redis");
var async = require("async");
var Stratum = require("meter-stratum-pool");
var util = require("meter-stratum-pool/lib/util.js");
var meterify = require("meterify").meterify;
var Web3 = require("web3");
const { default: BigNumber } = require("bignumber.js");

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
  var beneficiary = "0x" + poolOptions.rewardBeneficiary;

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

  var rewardInterval;

  console.log("SETUP ");
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
        /* Call redis to get an array of rounds - which are coinbase transactions and block heights from submitted
               blocks. */
        function (callback) {
          startRedisTimer();
          logger.debug(
            logSystem,
            logComponent,
            "Call redis to get an array of rounds"
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
              var workers = {};
              for (var w in results[0]) {
                workers[w] = {
                  balance: parseFloat(results[0][w]),
                };
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

          let pendingShares = {};
          let totalShares = 0;

          logger.debug(
            logSystem,
            logComponent,
            "Load shares on each round, calculate totalShares & pendingShares"
          );
          startRedisTimer();
          redisClient
            .multi(shareLookups)
            .exec(function (error, allWorkerShares) {
              endRedisTimer();

              if (error) {
                callback(
                  "Check finished - redis error with multi get rounds share"
                );
                return;
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
                        return p + parseFloat(workerShares[c]);
                      },
                      0
                    );
                    totalShares += totalSharesInRound;

                    for (var workerAddress in workerShares) {
                      if (!(workerAddress in pendingShares)) {
                        pendingShares[workerAddress] = 0;
                      }
                      pendingShares[workerAddress] +=
                        workerShares[workerAddress];
                    }
                    break;
                }
              });
              logger.debug(
                logSystem,
                logComponent,
                "Total Shares: " +
                  totalShares +
                  ", PendingShares: " +
                  pendingShares.length
              );

              callback(null, workers, rounds, totalShares, pendingShares);
            });
        },

        /**
         *  Load balance on pool account and calculate actual reward amount for each miner
         *  according to their shares
         */
        function (workers, rounds, totalShares, pendingShares, callback) {
          logger.debug(
            logSystem,
            logComponent,
            "Loading MTR balance and calculate the actual reward amount for each worker"
          );
          web3.eth.getEnergy(beneficiary, function (bal) {
            let pendingReward = new BigNumber(bal);
            console.log("balance: ", bal);
            if (beneficiary in workers) {
              const selfBalance = new BigNumber(
                workers[beneficiary].balance || 0
              );
              const selfReward = new BigNumber(
                workers[beneficiary].reward || 0
              );
              const txFee = new BigNumber(5e16);
              console.log("self balance: ", selfBalance.toFixed(0));
              console.log("self reward: ", selfReward.toFixed(0));
              console.log("tx fee: ", txFee.toFixed(0));
              pendingReward = pendingReward
                .minus(selfBalance)
                .minus(selfReward)
                .minus(txFee);
            }
            if (pendingReward.isLessThanOrEqualTo(0)) {
              logger.debug(logSystem, logComponent, "Not enough reward");
              callback(
                "not enough reward: " + pendingReward.toFixed(0),
                workers,
                rounds
              );
            }

            logger.debug(
              logSystem,
              logComponent,
              "Pool has pending reward of " + pendingReward.toFixed(0)
            );

            for (let w in workers) {
              if (w in pendingShares) {
                if (!workers[w].reward) {
                  workers[w].reward = new BigNumber(0);
                } else {
                  workers[w].reward = new BigNumber(workers[w].reward);
                }
                if (!workers[w].balance) {
                  workers[w].balance = new BigNumber(0);
                } else {
                  workers[w].balance = new BigNumber(workers[w].balance);
                }

                const workerReward = pendingReward
                  .times(share)
                  .div(totalShares);
                if (w.match(ADDR_PATTERN)) {
                  workers[w].reward = workerReward.add(workers[w].reward);
                  workers[w].issue = true;
                } else {
                  workers[w].balance = workerReward.add(workers[w].balance);
                  workers[w].issue = false;
                }
              }
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
                  .add(worker.balance)
                  .add(worker.reward);
              }
            }

            if (Object.keys(addressAmounts).length === 0) {
              callback(null, workers, rounds);
              return;
            }

            logger.debug(logSystem, logComponent, "Prepare to send reward tx");
            web3.eth.getBlockNum(function (blockNum) {
              console.log("best num:", bestNum);
              web3.eth.getBlock(bestNum, function (best) {
                console.log(best);
                const blockRef = best.id.substr(0, 18);
                let chainTag = processingConfig.chainTag; // chainTag for testnet

                let dataGas = 0;
                let clauses = [];
                for (const addr in addressAmounts) {
                  const amount = addressAmounts[addr];
                  console.log(`pay to ${addr} with ${amount}`);
                  clauses.push({ to: addr, value: amount.toFixed(0) });
                }
                const baseGas = 5000 + clauses.length * 16000; // fixed value
                console.log("base gas: ", baseGas);
                console.log("data gas: ", dataGas);
                let txObj = {
                  chainTag,
                  blockRef, // the first 8 bytes of latest block
                  expiration: 48, // blockRefHeight + expiration is the height for tx expire
                  clauses,
                  gasPriceCoef: 0,
                  gas: baseGas + dataGas,
                  dependsOn: null,
                  nonce: getRandomInt(Number.MAX_SAFE_INTEGER), // random number
                };
                let tx = new Transaction(txObj);
                const pkBuffer = Buffer.from(pk.replace("0x", ""), "hex");
                const signingHash = cry.blake2b256(tx.encode());
                logger.debug(logSystem, logComponent, "Signed reward tx");
                tx.signature = cry.secp256k1.sign(signingHash, pkBuffer);

                const raw = tx.encode();
                const rawTx = "0x" + raw.toString("hex");
                logger.debug(logSystem, logComponent, "Sending out reward tx");
                web3.eth.sendSignedTransaction(rawTx, function () {
                  callback(null, workers, rounds);
                });
              });
            });
            // daemon.cmd(
            //   "sendmany",
            //   [addressAccount || "", addressAmounts],
            //   function (result) {
            //     //Check if rewards failed because wallet doesn't have enough coins to pay for tx fees
            //     if (result.error && result.error.code === -6) {
            //       var higherPercent = withholdPercent + 0.01;
            //       logger.warning(
            //         logSystem,
            //         logComponent,
            //         "Not enough funds to cover the tx fees for sending out rewards, decreasing rewards by " +
            //           higherPercent * 100 +
            //           "% and retrying"
            //       );
            //       trySend(higherPercent);
            //     } else if (result.error) {
            //       logger.error(
            //         logSystem,
            //         logComponent,
            //         "Error trying to send rewards with RPC sendmany " +
            //           JSON.stringify(result.error)
            //       );
            //       callback(true);
            //     } else {
            //       logger.debug(
            //         logSystem,
            //         logComponent,
            //         "Sent out a total of " +
            //           totalSent / magnitude +
            //           " to " +
            //           Object.keys(addressAmounts).length +
            //           " workers"
            //       );
            //       if (withholdPercent > 0) {
            //         logger.warning(
            //           logSystem,
            //           logComponent,
            //           "Had to withhold " +
            //             withholdPercent * 100 +
            //             "% of reward from miners to cover transaction fees. " +
            //             "Fund pool wallet with coins to prevent this from happening"
            //         );
            //       }
            //       callback(null, workers, rounds);
            //     }
            //   },
            //   true,
            //   true
            // );
          };
          trySend(0);
        },
        function (workers, rounds, callback) {
          var totalPaid = 0;

          var balanceUpdateCommands = [];
          var workerPayoutsCommand = [];

          for (var w in workers) {
            var worker = workers[w];
            if (worker.issue) {
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
              balanceUpdateCommands.push([
                "hset",
                coin + ":balances",
                w,
                worker.balance.toNumber(),
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
              totalPaid,
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
