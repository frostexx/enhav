const StellarSdk = require('stellar-sdk');
const WebSocket = require('ws');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const axios = require('axios');
const os = require('os');
const cluster = require('cluster');

/**
 * Enhanced Pi Network Transfer Flood Bot with high-precision timing
 * Designed to compete with Rust/Go implementations
 */
class PiNetworkTransferFloodBot {
    constructor({
        horizonUrls = ['https://api.mainnet.minepi.com', 'https://api.minepi.com'], // Multiple endpoints for redundancy
        networkPassphrase = 'Pi Network',
        sourcePassphrase = null,
        targetAddress = null,
        transferAmount = null,
        unlockTime = null,
        txCount = 50,                 // Higher number of transactions for flooding
        baseFee = 3200000,            // Higher base fee based on competitor analysis
        feeIncrement = 100000,        // Larger fee increment to ensure priority
        txSpacingMs = 2,              // Reduced spacing for faster flooding
        derivationPath = "m/44'/314159'/0'", // BIP44 derivation path for Pi Network
        parallelConnections = 5,      // Number of parallel API connections
        preFloodSeconds = 5,          // Seconds before unlock time to start flooding (increased)
        burstFactor = 3,              // Multiply transactions by this factor for bursting
        timeSyncIntervalMs = 10000,   // Time sync interval in ms
        precisionTimerThreshold = 10  // Seconds before unlock to switch to high precision timer
    }) {
        // Core settings
        this.horizonUrls = horizonUrls;
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
        this.timeSyncIntervalMs = timeSyncIntervalMs;
        this.precisionTimerThreshold = precisionTimerThreshold;
        
        // Initialize servers with multiple endpoints for redundancy
        this.servers = [];
        for (let i = 0; i < parallelConnections; i++) {
            const serverIndex = i % horizonUrls.length;
            this.servers.push(new StellarSdk.Server(horizonUrls[serverIndex]));
        }
        
        // Multi-threaded handling
        this.workerPool = [];
        this.cpuCount = os.cpus().length;
        
        // Enhanced timing system
        this.serverTimeOffset = 0; // Offset between local time and server time
        this.timeAccuracy = 0;     // Estimated accuracy of our time sync
        this.lastTimeSyncTime = 0; // Last time we synced with server
        
        // Block monitoring
        this.blockMonitoringActive = false;
        this.blockMonitorInterval = null;
        this.highPrecisionInterval = null;
        this.microPrecisionInterval = null;
        this.latestLedgerNum = 0;
        this.lastLedgerCloseTime = 0;
        this.avgBlockTimeMs = 5000; // Initial estimate, will be refined
        this.blockTimes = [];
        
        // Timers and alarm system
        this.timers = new Map(); // Multiple redundant timers
        this.alarmScheduled = false;
        
        // Bot state
        this.isRunning = false;
        this.transactions = [];
        this.preparedTransactions = false;
        this.submissionResults = [];
        this.sourceKeypair = null;
        this.wsConnections = [];
        this.retryAttempts = {};
        
        // Early preparation state to ensure we're ready
        this.earlyPreparationDone = false;
        
        console.log(`Initialized Enhanced Pi Network Transfer Flood Bot`);
        
        // Setup process-wide error handler to prevent crashes
        process.on('uncaughtException', (err) => {
            console.error('Uncaught exception:', err);
            // Try to continue running if possible
            this.emitLog('error', `Uncaught exception: ${err.message}. Bot will attempt to continue.`);
        });
    }
    
    /**
     * Initialize logger function for consistent logging
     * @param {Function} logFunction - External logger function
     */
    setLogger(logFunction) {
        this.externalLogger = logFunction;
    }
    
    /**
     * Emit log message to console and external logger if available
     */
    emitLog(type, message) {
        const logMessage = `[${new Date().toISOString()}] ${message}`;
        
        if (type === 'error') {
            console.error(logMessage);
        } else {
            console.log(logMessage);
        }
        
        if (this.externalLogger) {
            this.externalLogger({ message, type });
        }
    }
    
    /**
     * Synchronize with Pi Network time for accurate timing
     * This helps align our execution with the blockchain's time
     */
    async syncWithNetworkTime() {
        try {
            // Use multiple time sources for redundancy
            const timePromises = this.horizonUrls.map(url => 
                axios.get(`${url}/`, { timeout: 3000 })
                    .then(response => {
                        // Parse server time from response headers
                        const serverTime = new Date(response.headers.date).getTime();
                        return {
                            localTime: Date.now(),
                            serverTime
                        };
                    })
                    .catch(err => {
                        this.emitLog('error', `Error syncing time with ${url}: ${err.message}`);
                        return null;
                    })
            );
            
            const results = await Promise.all(timePromises);
            const validResults = results.filter(r => r !== null);
            
            if (validResults.length === 0) {
                throw new Error('Failed to sync with any time source');
            }
            
            // Calculate the median offset to reduce impact of outliers
            const offsets = validResults.map(r => r.serverTime - r.localTime);
            offsets.sort((a, b) => a - b);
            
            // Use median value for better accuracy
            const medianIndex = Math.floor(offsets.length / 2);
            this.serverTimeOffset = offsets[medianIndex];
            
            // Calculate accuracy as standard deviation of offsets
            const mean = offsets.reduce((sum, val) => sum + val, 0) / offsets.length;
            const squareDiffs = offsets.map(val => Math.pow(val - mean, 2));
            this.timeAccuracy = Math.sqrt(squareDiffs.reduce((sum, val) => sum + val, 0) / offsets.length);
            
            this.lastTimeSyncTime = Date.now();
            
            this.emitLog('info', `Time synchronized with Pi Network. Offset: ${this.serverTimeOffset}ms, Accuracy: ±${this.timeAccuracy.toFixed(2)}ms`);
        } catch (error) {
            this.emitLog('error', `Time synchronization failed: ${error.message}`);
            // If sync fails, we'll rely on local time
        }
    }
    
    /**
     * Get current server time accounting for network offset
     */
    getAdjustedTime() {
        // Add the server time offset to the local time
        return Date.now() + this.serverTimeOffset;
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
            this.emitLog('error', `Error deriving keypair: ${error.message}`);
            throw new Error(`Failed to derive keypair: ${error.message}`);
        }
    }

    /**
     * Initialize keypair from provided passphrase
     */
    async initializeKeypairs() {
        if (this.sourcePassphrase) {
            this.sourceKeypair = this.keypairFromPassphrase(this.sourcePassphrase);
            this.emitLog('info', `Source account: ${this.sourceKeypair.publicKey()}`);
        } else {
            throw new Error('Source passphrase is required');
        }

        if (!this.targetAddress) {
            throw new Error('Target address is required');
        }

        // Load account to check if it exists and is valid
        try {
            // Try all servers to ensure we can access the account
            let account = null;
            let lastError = null;
            
            for (const server of this.servers) {
                try {
                    account = await server.loadAccount(this.sourceKeypair.publicKey());
                    break; // Successfully loaded account
                } catch (e) {
                    lastError = e;
                    continue; // Try the next server
                }
            }
            
            if (!account && lastError) {
                throw lastError;
            }
            
            this.emitLog('info', `Successfully loaded source account`);
        } catch (error) {
            this.emitLog('error', `Error loading source account: ${error.message}`);
            throw new Error(`Failed to load source account: ${error.message}`);
        }

        // Validate target address
        try {
            StellarSdk.Keypair.fromPublicKey(this.targetAddress);
            this.emitLog('info', `Target address is valid`);
        } catch (error) {
            this.emitLog('error', `Invalid target address: ${error.message}`);
            throw new Error(`Invalid target address: ${error.message}`);
        }
    }
    
    /**
     * Setup multi-threaded worker pool for transaction submission
     * This allows parallelization across CPU cores
     */
    setupWorkerPool() {
        // Only setup workers if we're in the main thread
        if (!isMainThread) return;
        
        // Clear existing worker pool
        if (this.workerPool.length > 0) {
            this.workerPool.forEach(worker => worker.terminate());
            this.workerPool = [];
        }
        
        // Determine optimal worker count (leave one core for main thread)
        const workerCount = Math.max(1, this.cpuCount - 1);
        
        this.emitLog('info', `Setting up worker pool with ${workerCount} workers for parallel transaction submission`);
        
        // Create workers
        for (let i = 0; i < workerCount; i++) {
            const worker = new Worker(__filename, {
                workerData: {
                    workerId: i,
                    horizonUrls: this.horizonUrls,
                    networkPassphrase: this.networkPassphrase
                }
            });
            
            worker.on('message', (message) => {
                if (message.type === 'result') {
                    // Process transaction result from worker
                    this.processWorkerResult(message.data);
                } else if (message.type === 'log') {
                    // Forward log from worker
                    this.emitLog(message.logType || 'info', `[Worker ${i}] ${message.message}`);
                }
            });
            
            worker.on('error', (err) => {
                this.emitLog('error', `Worker ${i} error: ${err.message}`);
                // Restart worker on error
                worker.terminate();
                this.setupWorkerReplacement(i);
            });
            
            worker.on('exit', (code) => {
                if (code !== 0) {
                    this.emitLog('error', `Worker ${i} exited with code ${code}`);
                    this.setupWorkerReplacement(i);
                }
            });
            
            this.workerPool.push(worker);
        }
    }
    
    /**
     * Replace a worker that has crashed or exited
     */
    setupWorkerReplacement(index) {
        if (!this.isRunning) return;
        
        this.emitLog('info', `Replacing worker ${index}`);
        
        const worker = new Worker(__filename, {
            workerData: {
                workerId: index,
                horizonUrls: this.horizonUrls,
                networkPassphrase: this.networkPassphrase
            }
        });
        
        worker.on('message', (message) => {
            if (message.type === 'result') {
                this.processWorkerResult(message.data);
            } else if (message.type === 'log') {
                this.emitLog(message.logType || 'info', `[Worker ${index}] ${message.message}`);
            }
        });
        
        worker.on('error', (err) => {
            this.emitLog('error', `Worker ${index} error: ${err.message}`);
            worker.terminate();
            this.setupWorkerReplacement(index);
        });
        
        worker.on('exit', (code) => {
            if (code !== 0) {
                this.emitLog('error', `Worker ${index} exited with code ${code}`);
                this.setupWorkerReplacement(index);
            }
        });
        
        this.workerPool[index] = worker;
    }
    
    /**
     * Process transaction submission result from worker
     */
    processWorkerResult(result) {
        if (result.success) {
            this.emitLog('info', `✅ Transaction ${result.index + 1} SUCCESSFUL! Hash: ${result.hash}, Fee: ${result.fee}`);
            
            this.submissionResults.push({
                success: true,
                index: result.index,
                fee: result.fee,
                hash: result.hash,
                timestamp: result.timestamp
            });
            
            // If we've succeeded, start slowing down additional submissions
            if (this.submissionResults.filter(r => r.success).length > 0) {
                // We keep running but reduce frequency to avoid unnecessary network load
                this.txSpacingMs = Math.max(this.txSpacingMs, 20);
            }
        } else {
            this.emitLog('error', `❌ Transaction ${result.index + 1} FAILED (attempt ${result.attempt}/${result.maxAttempts}). Fee: ${result.fee}, Error: ${result.error}`);
            
            // Only retry if we haven't had a successful transaction yet
            if (this.submissionResults.filter(r => r.success).length === 0 && result.shouldRetry) {
                // Schedule retry with a worker
                this.scheduleRetry(result);
            }
        }
    }
    
    /**
     * Schedule a transaction retry using the worker pool
     */
    scheduleRetry(result) {
        const backoffMs = Math.min(50 * Math.pow(1.5, result.attempt), 2000);
        
        setTimeout(() => {
            if (!this.isRunning) return;
            
            // Pick a worker using round-robin
            const workerIndex = result.index % this.workerPool.length;
            const worker = this.workerPool[workerIndex];
            
            if (worker) {
                worker.postMessage({
                    type: 'retry-transaction',
                    data: result
                });
            }
        }, backoffMs);
    }

    /**
     * Start monitoring blocks to estimate block time and detect approach to unlock time
     */
    async startBlockMonitoring() {
        if (this.blockMonitoringActive) return;

        this.emitLog('info', 'Starting block monitoring...');
        this.blockMonitoringActive = true;
        
        // Sync time with network first
        await this.syncWithNetworkTime();

        // Start periodic time synchronization
        this.timeSyncInterval = setInterval(() => {
            this.syncWithNetworkTime();
        }, this.timeSyncIntervalMs);

        // Get current ledger info with failover between servers
        let latestLedger = null;
        
        for (const server of this.servers) {
            try {
                latestLedger = await server.ledgers().order('desc').limit(1).call();
                if (latestLedger && latestLedger.records && latestLedger.records.length > 0) {
                    break;
                }
            } catch (e) {
                this.emitLog('error', `Error fetching ledger from server: ${e.message}`);
                continue;
            }
        }
        
        if (!latestLedger || !latestLedger.records || latestLedger.records.length === 0) {
            throw new Error('Failed to fetch ledger from any server');
        }
        
        this.latestLedgerNum = latestLedger.records[0].sequence;
        this.lastLedgerCloseTime = new Date(latestLedger.records[0].closed_at).getTime();

        this.emitLog('info', `Current ledger: ${this.latestLedgerNum}, closed at: ${latestLedger.records[0].closed_at}`);

        // Setup WebSocket connections for multiple servers to increase robustness
        this.setupWebSocketConnections();

        // Set up normal-precision monitoring (1-second interval)
        this.blockMonitorInterval = setInterval(async () => {
            this.checkBlockAndUnlockTime();
        }, 1000);
        
        // Schedule the high-precision timers immediately if needed
        this.scheduleHighPrecisionTimers();

        return this;
    }
    
    /**
     * Check current block and proximity to unlock time
     * This is called by the timer and can also be called directly
     */
    async checkBlockAndUnlockTime() {
        if (!this.blockMonitoringActive) return;
        
        try {
            // Skip HTTP poll if WebSockets are connected
            if (this.wsConnections.some(conn => conn && conn.readyState === WebSocket.OPEN)) {
                // Still check unlock time even if we don't need to poll
                this.checkUnlockTimeProximity();
                return;
            }
            
            // Poll using HTTP as fallback
            let ledger = null;
            let errorCount = 0;
            
            for (const server of this.servers) {
                try {
                    ledger = await server.ledgers().order('desc').limit(1).call();
                    if (ledger && ledger.records && ledger.records.length > 0) {
                        break;
                    }
                } catch (e) {
                    errorCount++;
                    continue;
                }
            }
            
            if (errorCount === this.servers.length) {
                throw new Error('All servers failed to respond');
            }
            
            if (!ledger || !ledger.records || ledger.records.length === 0) {
                throw new Error('Invalid ledger response');
            }
            
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
                
                this.emitLog('info', `New ledger: ${currentLedgerNum}, block time: ${blockTime}ms, avg: ${this.avgBlockTimeMs}ms`);
                
                this.latestLedgerNum = currentLedgerNum;
                this.lastLedgerCloseTime = currentCloseTime;
            }
            
            // Always check unlock time proximity, regardless of new block
            this.checkUnlockTimeProximity();
        } catch (error) {
            this.emitLog('error', `Error monitoring blocks: ${error.message}`);
            // Try to reconnect WebSockets
            this.setupWebSocketConnections();
        }
    }
    
    /**
     * Schedule high precision timers for improved accuracy
     */
    scheduleHighPrecisionTimers() {
        if (!this.unlockTime) return;
        
        const now = this.getAdjustedTime();
        const timeToUnlock = this.unlockTime - now;
        
        if (timeToUnlock <= 0) {
            // Already past unlock time, execute immediately
            this.executeFlood();
            return;
        }
        
        // Calculate seconds to unlock
        const secondsToUnlock = timeToUnlock / 1000;
        
        if (secondsToUnlock <= this.precisionTimerThreshold) {
            // Switch to higher precision interval when close to unlock time
            this.emitLog('info', `Switching to high-precision timing (${secondsToUnlock.toFixed(2)}s to unlock)`);
            
            // Clear existing high-precision interval if it exists
            if (this.highPrecisionInterval) {
                clearInterval(this.highPrecisionInterval);
            }
            
            // Set up higher-precision check (100ms interval)
            this.highPrecisionInterval = setInterval(() => {
                this.checkUnlockTimeHighPrecision();
            }, 100);
            
            // Schedule preparation of transactions if not already done
            if (!this.preparedTransactions && !this.earlyPreparationDone) {
                this.prepareTransactionsAsync();
            }
            
            // If we're very close (within 2 seconds), set up micro-precision timer
            if (secondsToUnlock <= 2) {
                this.scheduleMicroPrecisionTimers();
            }
        }
    }
    
    /**
     * Schedule micro-precision timers (10ms interval) for final approach
     */
    scheduleMicroPrecisionTimers() {
        if (this.microPrecisionInterval) {
            clearInterval(this.microPrecisionInterval);
        }
        
        this.emitLog('info', 'Activating micro-precision timing for final approach');
        
        // Set up micro-precision check (10ms interval)
        this.microPrecisionInterval = setInterval(() => {
            this.checkUnlockTimeMicroPrecision();
        }, 10);
        
        // Also set up a direct setTimeout as another backup mechanism
        const now = this.getAdjustedTime();
        const msToUnlock = Math.max(0, this.unlockTime - now - 50); // Start 50ms early to account for processing time
        
        if (msToUnlock > 0) {
            this.emitLog('info', `Also scheduling direct timer for ${msToUnlock}ms from now`);
            
            // Direct timer as backup
            setTimeout(() => {
                this.emitLog('info', 'Direct timer triggered for execution');
                this.executeFlood();
            }, msToUnlock);
        }
    }
    
    /**
     * High-precision check for unlock time proximity (100ms interval)
     */
    checkUnlockTimeHighPrecision() {
        if (!this.unlockTime || !this.isRunning) return;
        
        const now = this.getAdjustedTime();
        const timeToUnlock = this.unlockTime - now;
        
        // If unlock time is in the past or very close, start flood
        if (timeToUnlock <= 100) { // Execute if within 100ms
            this.emitLog('info', 'High-precision timer triggered for execution');
            
            // Clear interval to prevent multiple executions
            if (this.highPrecisionInterval) {
                clearInterval(this.highPrecisionInterval);
                this.highPrecisionInterval = null;
            }
            
            this.executeFlood();
        } else {
            // Still waiting, prepare transactions if not done
            if (!this.preparedTransactions) {
                this.prepareTransactionsAsync();
            }
            
            // If very close, switch to micro-precision
            if (timeToUnlock <= 2000 && !this.microPrecisionInterval) {
                this.scheduleMicroPrecisionTimers();
            }
        }
    }
    
    /**
     * Micro-precision check for unlock time (10ms interval)
     */
    checkUnlockTimeMicroPrecision() {
        if (!this.unlockTime || !this.isRunning) return;
        
        const now = this.getAdjustedTime();
        const timeToUnlock = this.unlockTime - now;
        
        // Execute if we're at or past the unlock time
        if (timeToUnlock <= 20) { // Execute if within 20ms
            this.emitLog('info', 'Micro-precision timer triggered for execution');
            
            // Clear intervals to prevent multiple executions
            if (this.microPrecisionInterval) {
                clearInterval(this.microPrecisionInterval);
                this.microPrecisionInterval = null;
            }
            
            this.executeFlood();
        }
    }
    
    /**
     * Set up redundant alarm system using multiple methods
     */
    setupRedundantAlarms() {
        if (!this.unlockTime || this.alarmScheduled) return;
        
        const now = this.getAdjustedTime();
        const timeToUnlock = this.unlockTime - now;
        
        if (timeToUnlock <= 0) {
            this.executeFlood();
            return;
        }
        
        this.alarmScheduled = true;
        
        // Calculate various offsets to create a spread of timers
        const timers = [
            { offset: -200, name: "Primary" },
            { offset: -150, name: "Secondary" },
            { offset: -100, name: "Tertiary" },
            { offset: -50, name: "Quaternary" },
            { offset: 0, name: "Exact" }
        ];
        
        // Set multiple timers with different offsets
        timers.forEach(timer => {
            const timerMs = Math.max(0, timeToUnlock + timer.offset);
            
            if (timerMs > 0) {
                this.emitLog('info', `Setting ${timer.name} alarm for ${timerMs}ms from now (offset: ${timer.offset}ms)`);
                
                const timerId = setTimeout(() => {
                    this.emitLog('info', `${timer.name} alarm triggered`);
                    this.executeFlood();
                }, timerMs);
                
                this.timers.set(timer.name, timerId);
            }
        });
    }

    /**
     * Setup WebSocket connections to Horizon servers
     */
    setupWebSocketConnections() {
        try {
            // Close existing connections
            this.wsConnections.forEach(conn => {
                if (conn && conn.readyState === WebSocket.OPEN) {
                    conn.close();
                }
            });
            
            this.wsConnections = [];
            
            // Create new connections - NOTE: Many Stellar/Pi Horizon servers don't support WebSockets
            // This is a fallback mechanism, and we rely primarily on HTTP polling
            try {
                // Use HTTP polling as primary mechanism, websockets as backup if available
                this.emitLog('info', "Using HTTP polling for block monitoring with WebSocket backup if available");
                
                // Try to set up websocket connections anyway as a backup
                // However, don't rely on them as primary mechanism
                for (const url of this.horizonUrls) {
                    try {
                        const wsUrl = url.replace('https://', 'wss://').replace('http://', 'ws://') + '/ledgers';
                        const ws = new WebSocket(wsUrl);
                        
                        ws.on('open', () => {
                            this.emitLog('info', `WebSocket connection established to ${wsUrl}`);
                        });
                        
                        ws.on('message', (data) => {
                            try {
                                const ledger = JSON.parse(data);
                                if (ledger && ledger.sequence) {
                                    this.latestLedgerNum = ledger.sequence;
                                    this.lastLedgerCloseTime = new Date(ledger.closed_at).getTime();
                                    
                                    // Check unlock time on each new block
                                    this.checkUnlockTimeProximity();
                                }
                            } catch (e) {
                                // Silent error - WebSocket is just a backup
                            }
                        });
                        
                        ws.on('error', (error) => {
                            // Silent error - WebSocket is just a backup
                        });
                        
                        this.wsConnections.push(ws);
                    } catch (e) {
                        // Skip if WebSocket connection fails
                    }
                }
            } catch (e) {
                // Skip if WebSockets are not supported
            }
        } catch (error) {
            this.emitLog('error', `Error setting up WebSocket connections: ${error.message}`);
        }
    }

    /**
     * Stop block monitoring
     */
    stopBlockMonitoring() {
        this.emitLog('info', 'Stopping block monitoring...');
        this.blockMonitoringActive = false;
        
        if (this.blockMonitorInterval) {
            clearInterval(this.blockMonitorInterval);
            this.blockMonitorInterval = null;
        }
        
        if (this.highPrecisionInterval) {
            clearInterval(this.highPrecisionInterval);
            this.highPrecisionInterval = null;
        }
        
        if (this.microPrecisionInterval) {
            clearInterval(this.microPrecisionInterval);
            this.microPrecisionInterval = null;
        }
        
        if (this.timeSyncInterval) {
            clearInterval(this.timeSyncInterval);
            this.timeSyncInterval = null;
        }
        
        // Clear all alarm timers
        for (const [name, timerId] of this.timers.entries()) {
            clearTimeout(timerId);
        }
        this.timers.clear();
        
        // Close WebSocket connections
        this.wsConnections.forEach(conn => {
            try {
                if (conn && conn.readyState === WebSocket.OPEN) {
                    conn.close();
                }
            } catch (e) {
                // Ignore errors when closing
            }
        });
        
        this.wsConnections = [];
    }

    /**
     * Check if we're approaching the unlock time and prepare for flood if so
     */
    checkUnlockTimeProximity() {
        if (!this.unlockTime || !this.isRunning) {
            return;
        }
        
        const now = this.getAdjustedTime();
        const timeToUnlock = this.unlockTime - now;
        
        // If unlock time is in the past, start flood immediately
        if (timeToUnlock <= 0) {
            this.emitLog('info', 'Unlock time has passed. Executing flood immediately.');
            this.executeFlood();
            return;
        }
        
        // Calculate seconds to unlock
        const secondsToUnlock = timeToUnlock / 1000;
        
        // Only log if time has changed significantly
        if (Math.floor(secondsToUnlock) % 5 === 0 || secondsToUnlock <= 10) {
            this.emitLog('info', `Time to unlock: ${secondsToUnlock.toFixed(2)} seconds`);
        }
        
        // If we're within the pre-flood window, prepare transactions
        if (secondsToUnlock <= this.preFloodSeconds && !this.transactions.length) {
            this.emitLog('info', `Within ${this.preFloodSeconds} seconds of unlock time. Preparing transactions...`);
            this.prepareTransactionsAsync();
            
            // Also set up redundant alarm system
            this.setupRedundantAlarms();
            
            // Switch to high-precision timing
            this.scheduleHighPrecisionTimers();
        }
    }

    /**
     * Prepare transactions asynchronously to avoid blocking
     */
    async prepareTransactionsAsync() {
        if (this.preparedTransactions || this.earlyPreparationDone) return;
        
        this.earlyPreparationDone = true;
        
        try {
            // Run preparation in a separate "thread" to avoid blocking
            setTimeout(async () => {
                try {
                    await this.prepareTransactions();
                } catch (error) {
                    this.emitLog('error', `Error preparing transactions: ${error.message}`);
                    // Retry preparation if it failed
                    setTimeout(() => this.prepareTransactionsAsync(), 500);
                }
            }, 0);
        } catch (error) {
            this.emitLog('error', `Error scheduling transaction preparation: ${error.message}`);
        }
    }

    /**
     * Prepare transfer transactions in advance
     */
    async prepareTransactions() {
        if (this.preparedTransactions || this.transactions.length > 0) {
            this.emitLog('info', 'Transactions already prepared');
            return;
        }

        try {
            this.emitLog('info', 'Preparing transactions...');
            
            // Load account details with retry mechanism
            let account = null;
            let attempts = 0;
            const maxAttempts = 3;
            
            while (attempts < maxAttempts && !account) {
                attempts++;
                
                for (const server of this.servers) {
                    try {
                        account = await server.loadAccount(this.sourceKeypair.publicKey());
                        if (account) break;
                    } catch (e) {
                        // Try next server
                        continue;
                    }
                }
                
                if (!account && attempts < maxAttempts) {
                    // Wait before retry
                    await new Promise(resolve => setTimeout(resolve, 500));
                }
            }
            
            if (!account) {
                throw new Error('Failed to load account after multiple attempts');
            }
            
            const currentSequence = BigInt(account.sequenceNumber());
            const balance = account.balances.find(b => b.asset_type === 'native');
            
            if (!balance) {
                throw new Error('No native balance found');
            }
            
            this.emitLog('info', `Current account balance: ${balance.balance} Pi`);
            
            // Calculate amount to transfer if not specified
            const transferAmount = this.transferAmount || parseFloat(balance.balance) * 0.95; // 95% of balance by default
            this.emitLog('info', `Using transfer amount: ${transferAmount} Pi`);
            
            // Use a dynamic strategy for fee distribution:
            // - Start with the base fee
            // - Use an exponential increase for higher priority transactions
            // - Prepare more transactions than needed for redundancy
            
            const feeStrategy = [];
            
            // First 10 transactions: base fee + small increment (ensuring some get through)
            for (let i = 0; i < 10; i++) {
                feeStrategy.push(this.baseFee + (i * this.feeIncrement * 0.5));
            }
            
            // Next 20 transactions: faster fee increase
            for (let i = 0; i < 20; i++) {
                feeStrategy.push(this.baseFee + this.feeIncrement + (i * i * this.feeIncrement * 0.2));
            }
            
            // Remaining transactions: exponential increase for max competitiveness
            for (let i = 0; i < this.txCount - 30; i++) {
                feeStrategy.push(this.baseFee + this.feeIncrement * 2 + (i * i * this.feeIncrement * 0.4));
            }
            
            // Prepare transactions with varying fees
            for (let i = 0; i < this.txCount; i++) {
                // Calculate fee for this transaction using our strategy
                const fee = Math.floor(feeStrategy[i] || (this.baseFee * 2 + (i * i * this.feeIncrement))).toString();
                
                // Create sequence-based account object
                const txAccount = new StellarSdk.Account(
                    this.sourceKeypair.publicKey(),
                    (currentSequence + BigInt(i)).toString()
                );
                
                // Create transaction with optimal settings
                const tx = new StellarSdk.TransactionBuilder(txAccount, {
                    fee,
                    networkPassphrase: this.networkPassphrase
                })
                .addOperation(StellarSdk.Operation.payment({
                    destination: this.targetAddress,
                    asset: StellarSdk.Asset.native(),
                    amount: transferAmount.toString()
                }))
                .setTimeout(60)  // Longer timeout for better chance of inclusion
                .build();
                
                // Sign transaction
                tx.sign(this.sourceKeypair);
                
                // Serialize transaction for efficient storage and worker transfer
                const serializedTx = tx.toXDR();
                
                // Store transaction with metadata
                this.transactions.push({
                    index: i,
                    fee,
                    serializedTx,
                    serverIndex: i % this.horizonUrls.length,
                    attempts: 0,
                    maxAttempts: 5 + Math.floor(i / 5)  // More attempts for higher fee transactions
                });
            }
            
            this.preparedTransactions = true;
            this.emitLog('info', `Successfully prepared ${this.transactions.length} transactions with fees from ${this.transactions[0].fee} to ${this.transactions[this.transactions.length - 1].fee} stroops`);
        } catch (error) {
            this.emitLog('error', `Error preparing transactions: ${error.message}`);
            throw error;
        }
    }

    /**
     * Execute the transaction flood using distributed and concurrent approach
     */
    async executeFlood() {
        // Use atomic check-and-set to ensure we only execute once
        if (this._floodExecuted) {
            return;
        }
        this._floodExecuted = true;
        
        try {
            this.emitLog('info', '🚀 EXECUTING TRANSFER FLOOD!');
            
            // Make sure we're marked as running
            this.isRunning = true;
            
            // Clear any existing timers to prevent duplicate execution
            this.clearAllTimers();
            
            // Make sure we have transactions prepared
            if (this.transactions.length === 0) {
                this.emitLog('info', 'No transactions prepared. Preparing now...');
                await this.prepareTransactions();
            }
            
            // Set up the worker pool if not already done
            if (this.workerPool.length === 0) {
                this.setupWorkerPool();
            }
            
            // Create bursts of transactions for optimal network flooding
            const burstCount = 3;  // Number of bursts
            const burstDelay = 50; // Milliseconds between bursts (reduced from original)
            
            // Create submission function using worker pool
            const submitTransaction = (txInfo) => {
                // Early exit if we've already had a success
                if (this.submissionResults.some(r => r.success)) {
                    // Slow down if we've already succeeded
                    return;
                }
                
                // Choose worker using round-robin distribution
                const workerIndex = txInfo.index % this.workerPool.length;
                const worker = this.workerPool[workerIndex];
                
                if (worker) {
                    worker.postMessage({
                        type: 'submit-transaction',
                        data: {
                            index: txInfo.index,
                            fee: txInfo.fee,
                            serializedTx: txInfo.serializedTx,
                            serverUrl: this.horizonUrls[txInfo.serverIndex % this.horizonUrls.length],
                            attempts: 0,
                            maxAttempts: txInfo.maxAttempts
                        }
                    });
                } else {
                    // Fallback to direct submission if worker is not available
                    this.submitTransactionDirect(txInfo);
                }
            };
            
            // Execute bursts of transactions
            for (let burst = 0; burst < burstCount; burst++) {
                setTimeout(() => {
                    this.emitLog('info', `Executing burst ${burst + 1}/${burstCount}...`);
                    
                    // Submit all transactions in this burst with minimal spacing
                    // Start with the highest fee transactions for best chances
                    // We sort transactions by fee (descending) to prioritize higher fees
                    const sortedTransactions = [...this.transactions].sort((a, b) => parseInt(b.fee) - parseInt(a.fee));
                    
                    // Submit transactions with dynamic spacing based on system capabilities
                    for (let i = 0; i < sortedTransactions.length; i++) {
                        const txInfo = sortedTransactions[i];
                        setTimeout(() => {
                            if (this.isRunning) {
                                submitTransaction(txInfo);
                            }
                        }, i * this.txSpacingMs);
                    }
                }, burst * burstDelay);
            }
            
            // Set up a monitor to detect success and stop flooding
            this.floodMonitorInterval = setInterval(() => {
                const successfulTxs = this.submissionResults.filter(r => r.success);
                
                if (successfulTxs.length > 0) {
                    this.emitLog('info', `Successfully transferred Pi with ${successfulTxs.length} transactions. First success: ${successfulTxs[0].hash}`);
                    
                    // Stop the flood after a brief delay to allow other high-priority txs to complete
                    setTimeout(() => {
                        this.stop();
                    }, 2000);
                    
                    clearInterval(this.floodMonitorInterval);
                }
            }, 500);
            
            // Safety timeout - stop flooding after 10 seconds regardless
            setTimeout(() => {
                if (this.isRunning) {
                    this.emitLog('info', 'Safety timeout reached. Stopping flood.');
                    this.stop();
                }
            }, 10000);
            
        } catch (error) {
            this.emitLog('error', `Error executing flood: ${error.message}`);
            // Continue running despite errors - the workers will handle retries
        }
    }
    
    /**
     * Fallback direct transaction submission if worker is not available
     */
    async submitTransactionDirect(txInfo) {
        try {
            // Deserialize transaction
            const tx = StellarSdk.TransactionBuilder.fromXDR(
                txInfo.serializedTx, 
                this.networkPassphrase
            );
            
            // Choose server
            const serverUrl = this.horizonUrls[txInfo.serverIndex % this.horizonUrls.length];
            const server = new StellarSdk.Server(serverUrl);
            
            this.emitLog('info', `Submitting tx ${txInfo.index + 1} directly with fee: ${txInfo.fee} stroops`);
            
            // Submit transaction
            const result = await server.submitTransaction(tx);
            
            // Process result
            this.emitLog('info', `✅ Transaction ${txInfo.index + 1} SUCCESSFUL! Hash: ${result.hash}, Fee: ${txInfo.fee}`);
            
            this.submissionResults.push({
                success: true,
                index: txInfo.index,
                fee: txInfo.fee,
                hash: result.hash,
                timestamp: new Date().toISOString()
            });
            
        } catch (error) {
            // Log error but don't stop execution
            let errorDetail = '';
            try {
                if (error.response && error.response.data) {
                    const responseData = error.response.data;
                    errorDetail = responseData.extras ? 
                        `${responseData.extras.result_codes.transaction}: ${JSON.stringify(responseData.extras.result_codes.operations)}` :
                        responseData.detail || '';
                }
            } catch (e) {
                errorDetail = error.message || 'Unknown error';
            }
            
            this.emitLog('error', `❌ Transaction ${txInfo.index + 1} FAILED (direct). Error: ${errorDetail}`);
        }
    }
    
    /**
     * Clear all timers to prevent duplicate execution
     */
    clearAllTimers() {
        if (this.blockMonitorInterval) {
            clearInterval(this.blockMonitorInterval);
            this.blockMonitorInterval = null;
        }
        
        if (this.highPrecisionInterval) {
            clearInterval(this.highPrecisionInterval);
            this.highPrecisionInterval = null;
        }
        
        if (this.microPrecisionInterval) {
            clearInterval(this.microPrecisionInterval);
            this.microPrecisionInterval = null;
        }
        
        // Clear all alarm timers
        for (const [name, timerId] of this.timers.entries()) {
            clearTimeout(timerId);
        }
        this.timers.clear();
    }

    /**
     * Initialize the bot and start monitoring
     */
    async start() {
        if (this.isRunning) {
            this.emitLog('info', 'Bot is already running');
            return;
        }
        
        this._floodExecuted = false;
        this.emitLog('info', `Starting Enhanced Pi Network Transfer Flood Bot`);
        this.isRunning = true;
        
        try {
            // Initialize keypairs first
            await this.initializeKeypairs();
            
            // Sync time with network
            await this.syncWithNetworkTime();
            
            // Start block monitoring
            await this.startBlockMonitoring();
            
            // Check if we need to execute immediately or prepare for future execution
            const now = this.getAdjustedTime();
            if (this.unlockTime && now >= this.unlockTime) {
                this.emitLog('info', `Unlock time has already passed. Executing flood immediately.`);
                await this.executeFlood();
            } else if (this.unlockTime) {
                const timeToUnlock = (this.unlockTime - now) / 1000;
                this.emitLog('info', `Unlock time is ${timeToUnlock.toFixed(2)} seconds in the future. Standing by...`);
                
                // If unlock is very soon, prepare transactions now
                if (timeToUnlock < 20) {
                    this.emitLog('info', 'Unlock time is close. Preparing transactions now...');
                    await this.prepareTransactions();
                    
                    // Also set up precision timers
                    this.scheduleHighPrecisionTimers();
                    
                    // And redundant alarms
                    this.setupRedundantAlarms();
                } else if (timeToUnlock < 60) {
                    // For times under 1 minute, start early preparation
                    this.prepareTransactionsAsync();
                }
            } else {
                this.emitLog('info', 'No unlock time specified. Execute flood manually or set unlock time.');
            }
            
            // Set up worker pool
            this.setupWorkerPool();
            
        } catch (error) {
            this.emitLog('error', `Error starting bot: ${error.message}`);
            this.stop();
            throw error;
        }
    }

    /**
     * Stop the bot
     */
    stop() {
        this.emitLog('info', 'Stopping bot...');
        this.isRunning = false;
        this.stopBlockMonitoring();
        
        // Clear all intervals
        this.clearAllTimers();
        
        // Stop workers
        if (this.workerPool.length > 0) {
            this.workerPool.forEach(worker => {
                try {
                    worker.terminate();
                } catch (e) {
                    // Ignore errors when terminating
                }
            });
            this.workerPool = [];
        }

        // Display summary of results
        const successful = this.submissionResults.filter(r => r.success);
        if (successful.length > 0) {
            this.emitLog('info', `\n===== SUMMARY =====`);
            this.emitLog('info', `Successful transactions: ${successful.length}/${this.transactions.length}`);
            
            if (successful.length > 0) {
                const firstSuccess = successful.sort((a, b) => 
                    new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
                )[0];
                
                this.emitLog('info', `First successful transaction: Index ${firstSuccess.index + 1}, Fee: ${firstSuccess.fee} stroops, Hash: ${firstSuccess.hash}`);
            }
        } else {
            this.emitLog('info', '\n===== SUMMARY =====');
            this.emitLog('info', 'No successful transactions. The bot might have been stopped before completion or the transactions failed.');
        }
    }
}

// Worker thread handler
if (!isMainThread) {
    const { workerId, horizonUrls, networkPassphrase } = workerData;
    
    // Store servers for the worker
    const servers = horizonUrls.map(url => new StellarSdk.Server(url));
    
    // Listen for messages from main thread
    parentPort.on('message', async (message) => {
        try {
            if (message.type === 'submit-transaction') {
                await handleSubmitTransaction(message.data);
            } else if (message.type === 'retry-transaction') {
                await handleSubmitTransaction(message.data);
            }
        } catch (err) {
            parentPort.postMessage({
                type: 'log',
                logType: 'error',
                message: `Worker error: ${err.message}`
            });
        }
    });
    
    // Handle transaction submission in the worker
    async function handleSubmitTransaction(data) {
        try {
            const { index, fee, serializedTx, serverUrl, attempts, maxAttempts } = data;
            
            // Find the correct server or create a new one
            let server = servers.find(s => s._serverURL === serverUrl);
            if (!server) {
                server = new StellarSdk.Server(serverUrl);
                servers.push(server);
            }
            
            // Deserialize transaction
            const tx = StellarSdk.TransactionBuilder.fromXDR(
                serializedTx, 
                networkPassphrase
            );
            
            // Log submission attempt
            parentPort.postMessage({
                type: 'log',
                message: `Submitting tx ${index + 1} with fee: ${fee} stroops (attempt ${attempts + 1}/${maxAttempts})`
            });
            
            // Submit transaction
            const result = await server.submitTransaction(tx);
            
            // Report success to main thread
            parentPort.postMessage({
                type: 'result',
                data: {
                    success: true,
                    index,
                    fee,
                    hash: result.hash,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            // Extract error details
            let errorDetail = '';
            let shouldRetry = true;
            
            try {
                if (error.response && error.response.data) {
                    const responseData = error.response.data;
                    
                    // Parse error codes
                    if (responseData.extras && responseData.extras.result_codes) {
                        errorDetail = `${responseData.extras.result_codes.transaction}`;
                        
                        // Detect permanent errors that shouldn't be retried
                        const txCode = responseData.extras.result_codes.transaction;
                        if (txCode === 'tx_bad_seq' || txCode === 'tx_insufficient_balance') {
                            shouldRetry = false;
                        }
                    } else {
                        errorDetail = responseData.detail || '';
                    }
                }
            } catch (e) {
                errorDetail = error.message || 'Unknown error';
            }
            
            const nextAttempt = (data.attempts || 0) + 1;
            const shouldRetryThisTime = shouldRetry && nextAttempt < data.maxAttempts;
            
            // Report failure to main thread
            parentPort.postMessage({
                type: 'result',
                data: {
                    success: false,
                    index: data.index,
                    fee: data.fee,
                    error: errorDetail,
                    attempt: nextAttempt,
                    maxAttempts: data.maxAttempts,
                    shouldRetry: shouldRetryThisTime
                }
            });
        }
    }
}

module.exports = PiNetworkTransferFloodBot;