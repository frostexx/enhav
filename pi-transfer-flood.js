/**
 * Pi Network Transfer Flood Bot - Enhanced Version with Rate Limiting
 * 
 * High-performance transaction flooding bot with hyper-precise timing mechanisms
 * designed to outperform competing bots written in Rust and Go.
 * 
 * Key improvements:
 * - Multiple redundant timing mechanisms with nanosecond precision targeting
 * - Rate limiting mechanisms to prevent API throttling
 * - Aggressive pre-warming and caching of network connections
 * - Multi-phase transaction flooding with dynamic fee escalation
 * - Advanced transaction preparation and parallelization
 * - Fail-safe execution with multiple backup triggers
 */

const StellarSdk = require('stellar-sdk');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const WebSocket = require('ws');
const { performance } = require('perf_hooks');

// Import a high-resolution timer library
const hrtime = process.hrtime;

class PiNetworkTransferFloodBot {
    constructor({
        horizonUrl = 'https://api.mainnet.minepi.com',
        networkPassphrase = 'Pi Network',
        sourcePassphrase = null,
        targetAddress = null,
        transferAmount = null,
        unlockTime = null,
        txCount = 50,                // Reduced from 100 to avoid rate limiting
        baseFee = 3500000,           // Higher starting fee based on competitor analysis
        feeIncrement = 200000,       // Larger fee increment to ensure priority
        txSpacingMs = 10,            // Increased from 2ms to avoid rate limiting
        derivationPath = "m/44'/314159'/0'",
        parallelConnections = 2,     // Reduced from 10 to avoid rate limiting
        preFloodSeconds = 5,         // Start preparing earlier
        burstFactor = 3,             // Reduced from 5 to avoid rate limiting
        maxRetries = 10,             // More persistent retries
        priorityFeeMultiplier = 2.5, // More aggressive fee multiplication for priority txs
        timingPrecision = true,      // Enable high-precision timing
        redundantTimers = true,      // Use multiple redundant timers
        proactivePreparation = true, // Pre-prepare transactions well in advance
        logLevel = 'info'            // Logging level: 'debug', 'info', 'warn', 'error'
    }) {
        // Core configuration
        this.horizonUrl = horizonUrl;
        this.networkPassphrase = networkPassphrase;
        this.sourcePassphrase = sourcePassphrase;
        this.targetAddress = targetAddress;
        this.transferAmount = transferAmount;
        this.txCount = txCount * burstFactor;
        this.baseFee = baseFee;
        this.feeIncrement = feeIncrement;
        this.txSpacingMs = txSpacingMs;
        this.derivationPath = derivationPath;
        this.preFloodSeconds = preFloodSeconds;
        this.unlockTime = unlockTime ? new Date(unlockTime).getTime() : null;
        this.parallelConnections = parallelConnections;
        this.maxRetries = maxRetries;
        this.priorityFeeMultiplier = priorityFeeMultiplier;
        
        // Advanced features
        this.timingPrecision = timingPrecision;
        this.redundantTimers = redundantTimers;
        this.proactivePreparation = proactivePreparation;
        this.logLevel = logLevel;
        
        // Initialize servers array
        this.servers = [];
        for (let i = 0; i < this.parallelConnections; i++) {
            this.servers.push(new StellarSdk.Server(this.horizonUrl));
        }
        
        // Bot state
        this.isRunning = false;
        this.isPrepared = false;
        this.isExecuting = false;
        this.floodExecuted = false;
        this.transactions = [];
        this.submissionResults = [];
        this.sourceKeypair = null;
        this.serverQueue = [...Array(this.parallelConnections).keys()]; // Server rotation queue
        
        // Block monitoring
        this.blockMonitoringActive = false;
        this.blockMonitorInterval = null;
        this.precisionInterval = null;
        this.directUnlockTimer = null;
        this.timerIDs = new Set();   // Track all timers for cleanup
        this.latestLedgerNum = 0;
        this.avgBlockTimeMs = 5000;  // Initial estimate
        this.blockTimes = [];
        
        // Websocket connections
        this.wsConnections = [];
        
        // Transaction statistics
        this.startTime = 0;
        this.endTime = 0;
        this.successCount = 0;
        this.failureCount = 0;
        
        // Cached account data
        this.accountData = null;
        this.accountSequence = null;
        this.accountDataTimestamp = 0;
        
        // Rate limiting
        this.lastSubmissionTime = 0;
        this.minTimeBetweenSubmissions = 100; // ms
        
        this.log('debug', `Initialized Pi Network Transfer Flood Bot (Enhanced)`);
    }
    
    // Enhanced logging with levels
    log(level, ...args) {
        const levels = {
            'debug': 0,
            'info': 1,
            'warn': 2,
            'error': 3
        };
        
        if (levels[level] >= levels[this.logLevel]) {
            const now = new Date();
            const timestamp = now.toISOString().replace('T', ' ').substring(0, 23);
            console.log(`[${timestamp}] [${level.toUpperCase()}]`, ...args);
        }
    }
    
    /**
     * Rate-limited API request with exponential backoff retry
     */
    async rateLimit(fn, maxRetries = 5, initialDelay = 1000) {
        let retries = 0;
        
        while (retries <= maxRetries) {
            try {
                return await fn();
            } catch (error) {
                if (error.response && error.response.status === 429) {
                    // Too Many Requests error
                    retries++;
                    if (retries > maxRetries) {
                        throw new Error(`Rate limit exceeded after ${maxRetries} retries`);
                    }
                    
                    // Calculate delay with exponential backoff
                    const delay = initialDelay * Math.pow(2, retries - 1);
                    this.log('info', `Rate limited. Retrying after ${delay}ms (attempt ${retries}/${maxRetries})`);
                    
                    // Wait before retrying
                    await new Promise(resolve => setTimeout(resolve, delay));
                } else {
                    // Different error, rethrow
                    throw error;
                }
            }
        }
    }
    
    /**
     * Derive Stellar keypair from a BIP39 mnemonic passphrase
     */
    keypairFromPassphrase(passphrase) {
        if (!passphrase) {
            throw new Error('Passphrase is required');
        }

        try {
            // Validate the mnemonic
            if (!bip39.validateMnemonic(passphrase)) {
                throw new Error('Invalid BIP39 mnemonic passphrase');
            }

            // Convert mnemonic to seed
            const seed = bip39.mnemonicToSeedSync(passphrase);

            // Derive the ED25519 key using the path
            const derivedKey = derivePath(this.derivationPath, seed.toString('hex'));

            // Create Stellar keypair from the derived private key
            return StellarSdk.Keypair.fromRawEd25519Seed(Buffer.from(derivedKey.key));
        } catch (error) {
            this.log('error', 'Error deriving keypair from passphrase:', error);
            throw new Error(`Failed to derive keypair: ${error.message}`);
        }
    }

    /**
     * Initialize keypair and validate accounts with rate limiting
     */
    async initializeKeypairs() {
        if (this.sourcePassphrase) {
            this.sourceKeypair = this.keypairFromPassphrase(this.sourcePassphrase);
            this.log('info', `Source account: ${this.sourceKeypair.publicKey()}`);
        } else {
            throw new Error('Source passphrase is required');
        }

        if (!this.targetAddress) {
            throw new Error('Target address is required');
        }

        // Use rate-limited approach to load account
        let accountLoaded = false;
        
        try {
            this.log('info', 'Loading account data (rate-limited)...');
            
            // Use rate limiting with the first server
            this.accountData = await this.rateLimit(
                () => this.servers[0].loadAccount(this.sourceKeypair.publicKey()),
                5,  // 5 retries maximum
                2000 // Start with 2 second delay, then exponential backoff
            );
            
            this.accountSequence = BigInt(this.accountData.sequenceNumber());
            this.accountDataTimestamp = Date.now();
            accountLoaded = true;
            this.log('info', `Successfully loaded source account with rate limiting`);
        } catch (error) {
            this.log('error', `Failed to load account with rate limiting: ${error.message}`);
            
            // Fallback to sequential attempts with fewer servers
            const maxServers = Math.min(3, this.servers.length);
            for (let i = 0; i < maxServers; i++) {
                try {
                    await new Promise(resolve => setTimeout(resolve, 2000 * i)); // Staggered delays
                    this.accountData = await this.servers[i].loadAccount(this.sourceKeypair.publicKey());
                    this.accountSequence = BigInt(this.accountData.sequenceNumber());
                    this.accountDataTimestamp = Date.now();
                    accountLoaded = true;
                    this.log('info', `Successfully loaded source account (using server ${i+1})`);
                    break;
                } catch (error) {
                    this.log('warn', `Error loading source account from server ${i+1}: ${error.message}`);
                }
            }
        }

        if (!accountLoaded) {
            throw new Error('Failed to load source account from any server');
        }

        // Validate target address
        try {
            StellarSdk.Keypair.fromPublicKey(this.targetAddress);
            this.log('info', 'Target address is valid');
        } catch (error) {
            this.log('error', 'Invalid target address:', error.message);
            throw new Error(`Invalid target address: ${error.message}`);
        }
    }
    
    /**
     * High-precision sleep function using a combination of setTimeout and busy waiting
     */
    preciseSleep(ms) {
        return new Promise(resolve => {
            const start = performance.now();
            const end = start + ms;
            
            // For short sleeps under 10ms, just busy wait
            if (ms < 10) {
                while (performance.now() < end) {
                    // Busy wait
                }
                resolve();
                return;
            }
            
            // For longer sleeps, use setTimeout for most of it, then busy wait for precision
            const busyWaitThreshold = 5; // Last 5ms will use busy waiting
            const timeoutMs = ms - busyWaitThreshold;
            
            const timeoutId = setTimeout(() => {
                // After timeout, busy wait for the remaining time for precision
                while (performance.now() < end) {
                    // Busy wait
                }
                resolve();
            }, timeoutMs);
            
            this.timerIDs.add(timeoutId);
        });
    }
    
    /**
     * Get next server in round-robin fashion
     */
    getNextServer() {
        // Rotate through servers
        const serverIndex = this.serverQueue.shift();
        this.serverQueue.push(serverIndex);
        return this.servers[serverIndex];
    }

    /**
     * Start monitoring blocks to estimate block time
     */
    async startBlockMonitoring() {
        if (this.blockMonitoringActive) return;

        this.log('info', 'Starting block monitoring...');
        this.blockMonitoringActive = true;

        // Get current ledger info from multiple servers for redundancy with rate limiting
        let ledgerLoaded = false;
        
        try {
            const latestLedger = await this.rateLimit(
                () => this.servers[0].ledgers().order('desc').limit(1).call(),
                3,
                1500
            );
            
            this.latestLedgerNum = latestLedger.records[0].sequence;
            this.lastLedgerCloseTime = new Date(latestLedger.records[0].closed_at).getTime();
            
            this.log('info', `Current ledger: ${this.latestLedgerNum}, closed at: ${latestLedger.records[0].closed_at}`);
            ledgerLoaded = true;
        } catch (error) {
            this.log('warn', `Failed to get ledger with rate limiting: ${error.message}`);
            
            // Try other servers with delays
            for (let i = 0; i < this.servers.length && !ledgerLoaded; i++) {
                try {
                    await new Promise(resolve => setTimeout(resolve, 1000 * i));
                    const latestLedger = await this.servers[i].ledgers().order('desc').limit(1).call();
                    this.latestLedgerNum = latestLedger.records[0].sequence;
                    this.lastLedgerCloseTime = new Date(latestLedger.records[0].closed_at).getTime();
                    
                    this.log('info', `Current ledger: ${this.latestLedgerNum}, closed at: ${latestLedger.records[0].closed_at} (from server ${i+1})`);
                    ledgerLoaded = true;
                    break;
                } catch (error) {
                    this.log('warn', `Failed to get ledger from server ${i+1}: ${error.message}`);
                }
            }
        }

        // Setup WebSocket connections for real-time updates
        this.setupWebSocketConnections();

        // Start standard block monitoring interval - with slightly longer interval to avoid rate limiting
        this.blockMonitorInterval = setInterval(async () => {
            if (!this.isRunning) return;
            
            try {
                // Get ledger updates with rate limiting
                let newLedgerFound = false;
                
                try {
                    const ledger = await this.rateLimit(
                        () => this.servers[0].ledgers().order('desc').limit(1).call(),
                        2,
                        1000
                    );
                    
                    const currentLedgerNum = ledger.records[0].sequence;
                    const currentCloseTime = new Date(ledger.records[0].closed_at).getTime();

                    // Only process if this is a new ledger
                    if (currentLedgerNum > this.latestLedgerNum) {
                        const blockTime = currentCloseTime - this.lastLedgerCloseTime;
                        this.blockTimes.push(blockTime);
                        
                        // Keep only the last 10 block times for average calculation
                        if (this.blockTimes.length > 10) {
                            this.blockTimes.shift();
                        }
                        
                        // Calculate average block time
                        this.avgBlockTimeMs = this.blockTimes.reduce((sum, time) => sum + time, 0) / this.blockTimes.length;
                        
                        this.log('debug', `New ledger: ${currentLedgerNum}, block time: ${blockTime}ms, avg: ${this.avgBlockTimeMs}ms`);
                        
                        this.latestLedgerNum = currentLedgerNum;
                        this.lastLedgerCloseTime = currentCloseTime;
                        newLedgerFound = true;
                        
                        // Check if we're approaching unlock time
                        this.checkUnlockTimeProximity();
                    }
                } catch (error) {
                    this.log('warn', `Error getting ledger with rate limiting: ${error.message}`);
                    
                    // Fallback to sequential checks with different servers
                    for (let i = 0; !newLedgerFound && i < this.servers.length; i++) {
                        try {
                            // Add delay between server calls to avoid rate limiting
                            if (i > 0) {
                                await new Promise(resolve => setTimeout(resolve, 500 * i));
                            }
                            
                            const ledger = await this.servers[i].ledgers().order('desc').limit(1).call();
                            const currentLedgerNum = ledger.records[0].sequence;
                            
                            if (currentLedgerNum > this.latestLedgerNum) {
                                // Update block information
                                const currentCloseTime = new Date(ledger.records[0].closed_at).getTime();
                                const blockTime = currentCloseTime - this.lastLedgerCloseTime;
                                
                                this.blockTimes.push(blockTime);
                                if (this.blockTimes.length > 10) {
                                    this.blockTimes.shift();
                                }
                                
                                this.avgBlockTimeMs = this.blockTimes.reduce((sum, time) => sum + time, 0) / this.blockTimes.length;
                                this.log('debug', `New ledger from server ${i+1}: ${currentLedgerNum}, block time: ${blockTime}ms`);
                                
                                this.latestLedgerNum = currentLedgerNum;
                                this.lastLedgerCloseTime = currentCloseTime;
                                newLedgerFound = true;
                                
                                // Check proximity to unlock time
                                this.checkUnlockTimeProximity();
                                break;
                            }
                        } catch (error) {
                            this.log('warn', `Error getting ledger from server ${i+1}: ${error.message}`);
                        }
                    }
                }
            } catch (error) {
                this.log('error', 'Error monitoring blocks:', error.message);
            }
        }, 2000); // Check every 2 seconds to reduce rate limiting (increased from 1 second)
        
        // Additional high-frequency proximity check for more precision
        this.precisionInterval = setInterval(() => {
            if (!this.isRunning) return;
            
            // Run more frequent unlock time checks when we're getting close
            const now = Date.now();
            if (this.unlockTime && Math.abs(this.unlockTime - now) < 10000) { // Within 10 seconds
                this.checkUnlockTimeProximity();
            }
        }, 200); // Check every 200ms when we're close (increased from 100ms)

        return this;
    }

    /**
     * Setup WebSocket connections to Horizon servers
     */
    setupWebSocketConnections() {
        try {
            // Close existing connections
            this.wsConnections.forEach(conn => {
                try {
                    if (conn && conn.readyState === WebSocket.OPEN) {
                        conn.close();
                    }
                } catch (e) {
                    this.log('warn', "Error closing WebSocket:", e.message);
                }
            });
            
            this.wsConnections = [];
            
            // Since Pi Network doesn't fully support WebSockets, we'll rely primarily on HTTP polling
            this.log('info', "Using HTTP polling as primary mechanism for block monitoring");
        } catch (error) {
            this.log('error', 'Error setting up WebSocket connections:', error.message);
        }
    }

    /**
     * Stop block monitoring and clean up
     */
    stopBlockMonitoring() {
        this.log('info', 'Stopping block monitoring...');
        this.blockMonitoringActive = false;
        
        // Clear all intervals
        if (this.blockMonitorInterval) {
            clearInterval(this.blockMonitorInterval);
            this.blockMonitorInterval = null;
        }
        
        if (this.precisionInterval) {
            clearInterval(this.precisionInterval);
            this.precisionInterval = null;
        }
        
        // Clear all timers
        this.timerIDs.forEach(timerId => {
            clearTimeout(timerId);
        });
        this.timerIDs.clear();
        
        // Close WebSocket connections
        this.wsConnections.forEach(conn => {
            try {
                if (conn && conn.readyState === WebSocket.OPEN) {
                    conn.close();
                }
            } catch (e) {
                this.log('warn', "Error closing WebSocket:", e.message);
            }
        });
        
        this.wsConnections = [];
    }

    /**
     * Enhanced unlock time proximity check with redundant timing mechanisms
     */
    checkUnlockTimeProximity() {
        if (!this.unlockTime || this.floodExecuted) {
            return;
        }
        
        const now = Date.now();
        const timeToUnlock = this.unlockTime - now;
        
        // If unlock time is in the past, start flood immediately
        if (timeToUnlock <= 0) {
            this.log('info', 'Unlock time has passed. Executing flood immediately.');
            this.executeFlood();
            return;
        }
        
        // Calculate seconds to unlock
        const secondsToUnlock = timeToUnlock / 1000;
        
        // Only log if more than 1 second changes to reduce noise
        if (Math.floor(secondsToUnlock) % 5 === 0) {
            this.log('debug', `Time to unlock: ${secondsToUnlock.toFixed(2)} seconds`);
        }
        
        // Prepare transactions well in advance
        if (this.proactivePreparation && !this.isPrepared && secondsToUnlock <= 30) {
            this.log('info', `Proactively preparing transactions ${secondsToUnlock.toFixed(2)} seconds before unlock time`);
            this.prepareTransactions();
        }
        
        // If we're within the pre-flood window, ensure transactions are prepared
        if (secondsToUnlock <= this.preFloodSeconds && !this.isPrepared) {
            this.log('info', `Within ${this.preFloodSeconds} seconds of unlock time. Preparing transactions...`);
            this.prepareTransactions();
        }
        
        // If we're very close to unlock time, prepare for execution with high precision timing
        if (secondsToUnlock <= 1.0 && !this.isExecuting) {
            this.log('info', `CRITICAL: ${secondsToUnlock.toFixed(3)} seconds to unlock - preparing for precision execution`);
            this.isExecuting = true;
            
            // Calculate milliseconds until flood start (slightly before unlock time)
            const msUntilFlood = Math.max(0, timeToUnlock - 50); // Start 50ms before to account for processing time
            
            this.log('info', `Scheduling flood to begin in ${msUntilFlood}ms with high-precision timer`);
            
            // Primary timer with precise timing
            if (this.timingPrecision) {
                const timerId = setTimeout(async () => {
                    this.log('info', 'PRECISION TIMER TRIGGERED - waiting for exact moment');
                    // Use busy-waiting for last few milliseconds
                    await this.preciseSleep(30);
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via precision timer');
                        this.executeFlood();
                    }
                }, msUntilFlood - 30);
                this.timerIDs.add(timerId);
            } else {
                const timerId = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via standard timer');
                        this.executeFlood();
                    }
                }, msUntilFlood);
                this.timerIDs.add(timerId);
            }
            
            // Add redundant timers with slight offsets if enabled
            if (this.redundantTimers) {
                // Earlier timer as backup (20ms before intended time)
                const earlyTimerId = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via early backup timer');
                        this.executeFlood();
                    }
                }, Math.max(0, msUntilFlood - 20));
                this.timerIDs.add(earlyTimerId);
                
                // Later timer as fallback (20ms after intended time)
                const lateTimerId = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via late fallback timer');
                        this.executeFlood();
                    }
                }, msUntilFlood + 20);
                this.timerIDs.add(lateTimerId);
                
                // Ultra-precise timer at exactly the right moment
                const preciseTimerId = setTimeout(async () => {
                    // Calculate exact nanoseconds to wait
                    const preciseNow = Date.now();
                    const preciseWait = Math.max(0, this.unlockTime - preciseNow - 5); // 5ms before unlock
                    
                    if (preciseWait > 0) {
                        await this.preciseSleep(preciseWait);
                    }
                    
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via ultra-precise timer');
                        this.executeFlood();
                    }
                }, Math.max(0, msUntilFlood - 50));
                this.timerIDs.add(preciseTimerId);
            }
        }
    }

    /**
     * Prepare transfer transactions in advance with optimized fee strategy
     * and rate limiting protection
     */
    async prepareTransactions() {
        if (this.isPrepared) {
            this.log('debug', 'Transactions already prepared');
            return;
        }

        try {
            this.log('info', 'Preparing transactions...');
            
            // Refresh account data with rate limiting if needed
            if (!this.accountData || Date.now() - this.accountDataTimestamp > 10000) {
                try {
                    this.log('info', 'Refreshing account data...');
                    this.accountData = await this.rateLimit(
                        () => this.servers[0].loadAccount(this.sourceKeypair.publicKey()),
                        3,  // 3 retries maximum
                        2000 // 2 second initial delay
                    );
                    this.accountSequence = BigInt(this.accountData.sequenceNumber());
                    this.accountDataTimestamp = Date.now();
                    this.log('debug', `Refreshed account data successfully`);
                } catch (error) {
                    this.log('warn', `Failed to refresh account data: ${error.message}`);
                    
                    // Try with different servers if rate limited
                    if (!this.accountData) {
                        let loaded = false;
                        for (let i = 0; i < this.servers.length && !loaded; i++) {
                            try {
                                await new Promise(resolve => setTimeout(resolve, 1000 * i));
                                this.accountData = await this.servers[i].loadAccount(this.sourceKeypair.publicKey());
                                this.accountSequence = BigInt(this.accountData.sequenceNumber());
                                this.accountDataTimestamp = Date.now();
                                loaded = true;
                                this.log('info', `Loaded account data from server ${i+1}`);
                            } catch (e) {
                                this.log('warn', `Failed to load account from server ${i+1}: ${e.message}`);
                            }
                        }
                        
                        if (!loaded) {
                            throw new Error('Failed to load account data from any server');
                        }
                    }
                }
            }
            
            const balance = this.accountData.balances.find(b => b.asset_type === 'native');
            
            if (!balance) {
                throw new Error('No native balance found');
            }
            
            this.log('info', `Current account balance: ${balance.balance} Pi`);
            
            // Calculate amount to transfer if not specified
            const transferAmount = this.transferAmount || parseFloat(balance.balance) * 0.98; // 98% of balance by default
            this.log('info', `Using transfer amount: ${transferAmount} Pi`);
            
            // Calculate a fee strategy that outbids competitors
            const feeLevels = this.calculateFeeStrategy();
            
            // Prepare transactions with varying fees
            for (let i = 0; i < this.txCount; i++) {
                // Calculate fee based on position in sequence
                // Use exponential fee increase for the most important transactions
                let fee;
                
                if (i < 10) {
                    // First 10 transactions get ultra-high fees (top 10%)
                    fee = Math.floor(feeLevels.ultra + this.feeIncrement * (i * 1.5)).toString();
                } else if (i < 30) {
                    // Next 20 get high fees (next 20%)
                    fee = Math.floor(feeLevels.high + this.feeIncrement * ((i - 10) * 0.8)).toString();
                } else if (i < 60) {
                    // Next 30 get medium fees (next 30%)
                    fee = Math.floor(feeLevels.medium + this.feeIncrement * ((i - 30) * 0.5)).toString();
                } else {
                    // Rest get base fee (remaining 40%)
                    fee = Math.floor(feeLevels.base + this.feeIncrement * ((i - 60) * 0.2)).toString();
                }
                
                // Create sequence-based account object
                const txAccount = new StellarSdk.Account(
                    this.sourceKeypair.publicKey(),
                    (this.accountSequence + BigInt(i)).toString()
                );
                
                // Create transaction
                const tx = new StellarSdk.TransactionBuilder(txAccount, {
                    fee,
                    networkPassphrase: this.networkPassphrase
                })
                .addOperation(StellarSdk.Operation.payment({
                    destination: this.targetAddress,
                    asset: StellarSdk.Asset.native(),
                    amount: transferAmount.toString()
                }))
                .setTimeout(30)  // Shorter timeout for faster processing
                .build();
                
                // Sign transaction
                tx.sign(this.sourceKeypair);
                
                // Store transaction with metadata
                this.transactions.push({
                    index: i,
                    fee,
                    tx,
                    serverIndex: i % this.parallelConnections,  // Distribute across servers
                    attempts: 0,
                    maxAttempts: this.maxRetries - Math.floor(i / 20),  // More attempts for higher fee transactions
                    priority: i < 20 ? 'high' : i < 50 ? 'medium' : 'low' // Priority level for retry logic
                });
            }
            
            this.log('info', `Successfully prepared ${this.transactions.length} transactions with fees from ${this.transactions[0].fee} to ${this.transactions[this.transactions.length - 1].fee} stroops`);
            this.isPrepared = true;
        } catch (error) {
            this.log('error', 'Error preparing transactions:', error);
            throw error;
        }
    }
    
    /**
     * Calculate an aggressive fee strategy based on desired percentiles
     */
    calculateFeeStrategy() {
        // Calculate fee levels to ensure we win the fee auction
        const ultraFee = Math.floor(this.baseFee * this.priorityFeeMultiplier * 1.5); // 150% of priority fee for top transactions
        const highFee = Math.floor(this.baseFee * this.priorityFeeMultiplier);        // Full priority fee for important transactions
        const mediumFee = Math.floor(this.baseFee * 1.2);                            // 120% of base fee for medium priority
        const baseFee = this.baseFee;                                                // Base fee for low priority
        
        return {
            ultra: ultraFee,
            high: highFee,
            medium: mediumFee,
            base: baseFee
        };
    }

    /**
     * Execute the transaction flood using distributed and concurrent approach
     * with enhanced timing precision, retry logic, and rate limiting
     */
    async executeFlood() {
        if (this.floodExecuted) {
            this.log('debug', 'Flood already executed');
            return;
        }
        
        this.floodExecuted = true;
        this.log('info', '🚀 EXECUTING TRANSFER FLOOD!');
        this.startTime = Date.now();
        
        if (!this.isPrepared) {
            this.log('info', 'Transactions not prepared. Preparing now...');
            await this.prepareTransactions();
        }
        
        // Create bursts of transactions
        const burstCount = 4;  // Number of bursts
        const burstDelay = 500;  // Increased from 75ms to 500ms to avoid rate limiting
        
        // Store successful transactions to avoid duplicates
        const successful = new Set();
        
        // Create submission function that can be reused
        const submitTransaction = async (txInfo) => {
            if (successful.has(txInfo.index) || !this.isRunning) {
                return;
            }
            
            // Add rate limiting between submissions
            const now = Date.now();
            const timeSinceLastSubmission = now - this.lastSubmissionTime;
            if (timeSinceLastSubmission < this.minTimeBetweenSubmissions) {
                await new Promise(resolve => setTimeout(resolve, this.minTimeBetweenSubmissions - timeSinceLastSubmission));
            }
            
            // Choose the server based on round-robin
            const server = this.getNextServer();
            
            try {
                this.log('debug', `Submitting tx ${txInfo.index + 1}/${this.transactions.length} with fee: ${txInfo.fee} stroops (priority: ${txInfo.priority})`);
                const result = await server.submitTransaction(txInfo.tx);
                this.lastSubmissionTime = Date.now();
                
                // Mark as successful and log
                successful.add(txInfo.index);
                this.successCount++;
                this.log('info', `✅ Transaction ${txInfo.index + 1} SUCCESSFUL! Hash: ${result.hash}, Fee: ${txInfo.fee}`);
                
                this.submissionResults.push({
                    success: true,
                    index: txInfo.index,
                    fee: txInfo.fee,
                    hash: result.hash,
                    timestamp: new Date().toISOString()
                });
                
                // If we've succeeded, we can stop the bot after a short delay
                // to allow for any other successful transactions to complete
                if (this.successCount === 1) {
                    setTimeout(() => {
                        if (this.isRunning) {
                            this.log('info', '🎉 Successfully transferred Pi. Stopping bot...');
                            this.stop();
                        }
                    }, 2000);
                }
            } catch (error) {
                txInfo.attempts++;
                this.failureCount++;
                this.lastSubmissionTime = Date.now();
                
                // Determine if we should retry based on error and priority
                let shouldRetry = txInfo.attempts < txInfo.maxAttempts;
                let isRateLimited = false;
                
                // Parse error from Stellar
                let errorDetail = '';
                try {
                    if (error.response) {
                        if (error.response.status === 429) {
                            isRateLimited = true;
                            errorDetail = 'Rate limited (429 Too Many Requests)';
                        } else if (error.response.data) {
                            const responseData = error.response.data;
                            errorDetail = responseData.extras ? 
                                `${responseData.extras.result_codes.transaction}: ${JSON.stringify(responseData.extras.result_codes.operations)}` :
                                responseData.detail || '';
                                
                            // If we got a "tx_bad_seq" error, no point retrying this specific tx
                            if (errorDetail.includes('tx_bad_seq')) {
                                shouldRetry = false;
                            }
                            
                            // If we got a "tx_fee_bump_inner_failed" or similar, reduce retries
                            if (errorDetail.includes('tx_fee_bump_inner_failed')) {
                                txInfo.maxAttempts = Math.min(txInfo.maxAttempts, txInfo.attempts + 2);
                            }
                        }
                    }
                } catch (e) {
                    errorDetail = error.message || 'Unknown error';
                }
                
                // Log the error with details
                this.log('debug', `❌ Transaction ${txInfo.index + 1} FAILED (attempt ${txInfo.attempts}/${txInfo.maxAttempts}). Fee: ${txInfo.fee}, Error: ${errorDetail}`);
                
                // If we should retry, schedule a retry with optimized backoff
                if (shouldRetry) {
                    // Prioritize retries for high-priority transactions and use longer backoff for rate limiting
                    const priorityMultiplier = txInfo.priority === 'high' ? 0.5 : 
                                              txInfo.priority === 'medium' ? 1.0 : 2.0;
                                              
                    // Use much longer backoff for rate limiting
                    const baseBackoff = isRateLimited ? 2000 : 100;
                    
                    // Use shorter backoff for higher priority transactions
                    const backoffMs = Math.min(baseBackoff * Math.pow(1.5, txInfo.attempts) * priorityMultiplier, 10000);
                    
                    setTimeout(() => {
                        if (this.isRunning && !successful.has(txInfo.index)) {
                            submitTransaction(txInfo);
                        }
                    }, backoffMs);
                }
            }
        };
        
        // Execute bursts of transactions
        for (let burst = 0; burst < burstCount; burst++) {
            setTimeout(() => {
                if (!this.isRunning) return;
                
                this.log('info', `Executing burst ${burst + 1}/${burstCount}...`);
                
                // Prioritize transactions based on burst
                let startIndex, endIndex;
                
                if (burst === 0) {
                    // First burst: highest priority transactions (first 25%)
                    startIndex = 0;
                    endIndex = Math.floor(this.transactions.length * 0.25);
                } else if (burst === 1) {
                    // Second burst: high priority transactions (next 25%)
                    startIndex = Math.floor(this.transactions.length * 0.25);
                    endIndex = Math.floor(this.transactions.length * 0.5);
                } else if (burst === 2) {
                    // Third burst: medium priority transactions (next 25%)
                    startIndex = Math.floor(this.transactions.length * 0.5);
                    endIndex = Math.floor(this.transactions.length * 0.75);
                } else {
                    // Final burst: remaining transactions
                    startIndex = Math.floor(this.transactions.length * 0.75);
                    endIndex = this.transactions.length;
                }
                
                // Submit all transactions in this burst with increased spacing to avoid rate limiting
                for (let i = startIndex; i < endIndex; i++) {
                    const txInfo = this.transactions[i];
                    
                    // Use varying delays based on priority and burst
                    const delay = i * this.txSpacingMs * (burst === 0 ? 1.0 : burst * 1.5);
                    
                    setTimeout(() => {
                        submitTransaction(txInfo);
                    }, delay);
                }
            }, burst * burstDelay);
        }
    }

    /**
     * Set up direct timers for unlock time to ensure execution
     * This operates independently from block monitoring
     */
    setupDirectUnlockTimer() {
        if (!this.unlockTime) {
            this.log('info', 'No unlock time specified. Skipping direct timer setup.');
            return;
        }
        
        const now = Date.now();
        const timeToUnlock = this.unlockTime - now;
        
        if (timeToUnlock <= 0) {
            this.log('info', 'Unlock time already passed. Executing immediately.');
            this.executeFlood();
            return;
        }
        
        this.log('info', `Setting up direct unlock timer for ${new Date(this.unlockTime).toISOString()} (${timeToUnlock}ms from now)`);
        
        // Create a tiered approach to execution
        
        // 1. Preparation timer - well in advance of unlock time
        const prepTime = Math.min(timeToUnlock - 5000, 30000); // 5 seconds before unlock or 30 seconds max
        if (prepTime > 0) {
            const prepTimerId = setTimeout(() => {
                if (!this.isPrepared) {
                    this.log('info', 'DIRECT TIMER: Preparing transactions in advance');
                    this.prepareTransactions();
                }
            }, Math.max(0, prepTime));
            this.timerIDs.add(prepTimerId);
        } else {
            // Prepare immediately if prep time is in the past
            this.prepareTransactions();
        }
        
        // 2. Primary execution timer - aimed at optimal execution time
        const primaryTime = timeToUnlock - 50; // 50ms before unlock
        if (primaryTime > 0) {
            const primaryTimerId = setTimeout(async () => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Primary execution timer triggered');
                    await this.preciseSleep(30); // Fine-tuned waiting
                    this.executeFlood();
                }
            }, Math.max(0, primaryTime - 30));
            this.timerIDs.add(primaryTimerId);
        }
        
        // 3. Backup timers - in case primary fails
        // First backup - slightly before primary
        const earlyBackupTime = timeToUnlock - 100; // 100ms before unlock
        if (earlyBackupTime > 0) {
            const earlyTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Early backup execution timer triggered');
                    this.executeFlood();
                }
            }, Math.max(0, earlyBackupTime));
            this.timerIDs.add(earlyTimerId);
        }
        
        // Second backup - at exactly unlock time
        const exactTimerId = setTimeout(() => {
            if (!this.floodExecuted) {
                this.log('info', 'DIRECT TIMER: Exact unlock time reached, executing');
                this.executeFlood();
            }
        }, Math.max(0, timeToUnlock));
        this.timerIDs.add(exactTimerId);
        
        // Third backup - slightly after
        const lateBackupTime = timeToUnlock + 50; // 50ms after unlock
        if (lateBackupTime > 0) {
            const lateTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Late backup execution timer triggered');
                    this.executeFlood();
                }
            }, Math.max(0, lateBackupTime));
            this.timerIDs.add(lateTimerId);
        }
        
        // 4. Safety nets - extra failsafes
        // First safety net - 0.5 seconds after unlock
        const safetyNet1 = timeToUnlock + 500;
        if (safetyNet1 > 0) {
            const safety1Id = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('warn', 'DIRECT TIMER: Safety net 1 triggered - 500ms after unlock');
                    this.executeFlood();
                }
            }, Math.max(0, safetyNet1));
            this.timerIDs.add(safety1Id);
        }
        
        // Second safety net - 2 seconds after unlock
        const safetyNet2 = timeToUnlock + 2000;
        if (safetyNet2 > 0) {
            const safety2Id = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('warn', 'DIRECT TIMER: EMERGENCY SAFETY NET - 2s after unlock, still not executed!');
                    this.executeFlood();
                } else {
                    this.log('info', 'Safety check passed - flood already executed');
                }
            }, Math.max(0, safetyNet2));
            this.timerIDs.add(safety2Id);
        }
        
        // Final watchdog timer
        const watchdogTimer = setTimeout(() => {
            if (!this.floodExecuted) {
                this.log('error', 'FATAL: All timers failed! Executing emergency flood!');
                this.executeFlood();
            }
        }, Math.max(0, timeToUnlock + 5000)); // 5 seconds after unlock
        this.timerIDs.add(watchdogTimer);
    }

    /**
     * Initialize the bot and start monitoring with rate limiting protection
     */
    async start() {
        if (this.isRunning) {
            this.log('info', 'Bot is already running');
            return;
        }
        
        this.log('info', `Starting Pi Network Transfer Flood Bot (Enhanced)`);
        this.isRunning = true;
        this.floodExecuted = false;
        this.isPrepared = false;
        this.isExecuting = false;
        
        try {
            // Initialize keypairs and validate accounts
            await this.initializeKeypairs();
            
            // Set up direct timers for unlock time (independent of block monitoring)
            this.setupDirectUnlockTimer();
            
            // Start block monitoring as a parallel mechanism
            await this.startBlockMonitoring();
            
            // Check if we need to execute immediately
            const now = Date.now();
            if (this.unlockTime && now >= this.unlockTime) {
                this.log('info', `Unlock time has already passed. Executing flood immediately.`);
                await this.executeFlood();
            } else if (this.unlockTime) {
                const timeToUnlock = (this.unlockTime - now) / 1000;
                this.log('info', `Unlock time is ${timeToUnlock.toFixed(2)} seconds in the future. Standing by...`);
                
                // Proactively prepare transactions for faster execution
                if (this.proactivePreparation && timeToUnlock < 30) {
                    this.log('info', 'Proactively preparing transactions for faster execution...');
                    await this.prepareTransactions();
                }
            } else {
                this.log('info', 'No unlock time specified. Execute flood manually or set unlock time.');
            }
            
            // Register process termination handlers
            process.on('SIGINT', () => {
                this.log('info', 'Received SIGINT signal. Shutting down...');
                this.stop();
            });
            
            process.on('SIGTERM', () => {
                this.log('info', 'Received SIGTERM signal. Shutting down...');
                this.stop();
            });
            
            return this;
        } catch (error) {
            this.log('error', 'Error starting bot:', error);
            this.stop();
            throw error;
        }
    }

    /**
     * Stop the bot and clean up resources
     */
    stop() {
        this.log('info', 'Stopping bot...');
        this.isRunning = false;
        this.stopBlockMonitoring();
        
        // Clear all timers
        this.timerIDs.forEach(timerId => {
            clearTimeout(timerId);
        });
        this.timerIDs.clear();

        // Display summary of results
        this.endTime = Date.now();
        const executionTime = this.endTime - this.startTime;
        
        if (this.startTime > 0) {
            this.log('info', `\n===== EXECUTION SUMMARY =====`);
            this.log('info', `Total execution time: ${executionTime}ms`);
            this.log('info', `Successful transactions: ${this.successCount}`);
            this.log('info', `Failed transactions: ${this.failureCount}`);
            
            const successful = this.submissionResults.filter(r => r.success);
            if (successful.length > 0) {
                const firstSuccess = successful.sort((a, b) => a.index - b.index)[0];
                this.log('info', `First successful transaction: Index ${firstSuccess.index + 1}, Fee: ${firstSuccess.fee} stroops, Hash: ${firstSuccess.hash}`);
                
                // Find the transaction with the lowest fee that succeeded
                const lowestFeeSuccess = successful.sort((a, b) => parseInt(a.fee) - parseInt(b.fee))[0];
                if (lowestFeeSuccess) {
                    this.log('info', `Lowest fee successful: ${lowestFeeSuccess.fee} stroops (Index ${lowestFeeSuccess.index + 1})`);
                }
            }
        }
    }
    
    /**
     * Get the current bot status and network information
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            isPrepared: this.isPrepared,
            isExecuting: this.isExecuting,
            floodExecuted: this.floodExecuted,
            transactionsPrepared: this.transactions.length,
            unlockTime: this.unlockTime ? new Date(this.unlockTime).toISOString() : null,
            timeToUnlock: this.unlockTime ? (this.unlockTime - Date.now()) / 1000 : null,
            successCount: this.successCount,
            failureCount: this.failureCount,
            latestLedgerNum: this.latestLedgerNum,
            lastLedgerTime: this.lastLedgerCloseTime ? new Date(this.lastLedgerCloseTime).toISOString() : null,
            avgBlockTimeMs: this.avgBlockTimeMs
        };
    }
}

module.exports = PiNetworkTransferFloodBot;