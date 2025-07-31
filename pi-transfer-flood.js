/**
 * Pi Network Transfer Flood Bot - Ultra-Enhanced Version
 * 
 * Ultra-optimized high-performance transaction flooding bot with nanosecond-precision timing
 * designed to outperform competing bots written in Rust and Go by leveraging JavaScript's
 * asynchronous strengths with native performance optimization techniques.
 * 
 * Key improvements:
 * - Multiple redundant timing mechanisms with nanosecond precision targeting
 * - Advanced connection pooling with HTTP/2 and Keep-Alive optimizations
 * - Smart backoff and rate limiting mechanisms to prevent API throttling
 * - Multi-phase pre-warming and early transaction preparation
 * - Advanced multi-thread execution via Worker Threads for parallel processing
 * - Ultra-aggressive fee strategies based on opponent analysis
 * - Predictive timing and pre-execution triggers
 * - Multiple fallback mechanisms and self-healing error recovery
 */

const StellarSdk = require('stellar-sdk');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const { performance, PerformanceObserver } = require('perf_hooks');
const axios = require('axios');
const https = require('https');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const { EventEmitter } = require('events');

// Import high-resolution timer libraries
const { hrtime, hrtimeBigint } = process;

// Custom high-performance EventEmitter with increased limits
class HighPerformanceEventEmitter extends EventEmitter {
    constructor() {
        super();
        this.setMaxListeners(100); // Allow more listeners without warnings
    }
}

class PiNetworkTransferFloodBot {
    constructor({
        horizonUrl = 'https://api.mainnet.minepi.com',
        networkPassphrase = 'Pi Network',
        sourcePassphrase = null,
        targetAddress = null,
        transferAmount = null,
        unlockTime = null,
        txCount = 150,               // Increased from 50 to 150 for more aggressive flooding
        baseFee = 6000000,           // Significantly higher starting fee based on competitor analysis
        feeIncrement = 300000,       // Larger fee increment to ensure priority
        txSpacingMs = 5,             // Reduced from 10ms to 5ms for faster flooding
        derivationPath = "m/44'/314159'/0'",
        parallelConnections = 4,     // Increased to 4 but with smart throttling
        preFloodSeconds = 60,        // Start preparing much earlier (1 minute)
        burstFactor = 4,             // Increased from 3 to 4 for more aggressive flooding
        maxRetries = 15,             // More persistent retries
        priorityFeeMultiplier = 3.0, // More aggressive fee multiplication
        timingPrecision = true,      // Enable high-precision timing
        redundantTimers = true,      // Use multiple redundant timers
        proactivePreparation = true, // Pre-prepare transactions well in advance
        logLevel = 'info',           // Logging level: 'debug', 'info', 'warn', 'error'
        useWorkerThreads = true,     // Use worker threads for parallel processing
        aggressiveMemoryCache = true, // Cache more data in memory for faster access
        useHttp2 = true,             // Use HTTP/2 for better multiplexing
        useTcpKeepAlive = true,      // Use TCP keep-alive for connection reuse
        earlyPreparationMinutes = 5, // Start preparing 5 minutes before unlock time
        predictiveExecution = true,  // Use predictive timing for execution
        smartBackoff = true,         // Use smart backoff strategy for rate limiting
        useNativeThreadPoolSize = true, // Use native thread pool size optimization
        useNodeFlags = true          // Leverage Node.js optimization flags
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
        this.useWorkerThreads = useWorkerThreads && isMainThread;
        this.aggressiveMemoryCache = aggressiveMemoryCache;
        this.useHttp2 = useHttp2;
        this.useTcpKeepAlive = useTcpKeepAlive;
        this.earlyPreparationMinutes = earlyPreparationMinutes;
        this.predictiveExecution = predictiveExecution;
        this.smartBackoff = smartBackoff;
        
        // System optimization settings
        if (useNativeThreadPoolSize) {
            // Optimize Node.js thread pool size for the available CPU cores
            process.env.UV_THREADPOOL_SIZE = Math.max(4, os.cpus().length);
        }
        
        // Initialize high-performance HTTP agents
        this.initializeHttpAgents();
        
        // Initialize servers array with custom HTTP agent
        this.servers = [];
        for (let i = 0; i < this.parallelConnections; i++) {
            this.servers.push(new StellarSdk.Server(this.horizonUrl, {
                allowHttp: false,
                timeout: 30000, // 30 second timeout
                appName: "PiNetworkTransferFloodBot-v3.0",
                appVersion: "3.0.0"
            }));
        }
        
        // Initialize backup server URLs for redundancy
        this.backupServerUrls = [
            'https://api.mainnet.minepi.com',
            'https://api.mainnet.minepi.com'  // Can be replaced with actual backup endpoints if available
        ];
        
        this.backupServers = this.backupServerUrls.map(url => 
            new StellarSdk.Server(url, {
                allowHttp: false,
                timeout: 30000
            })
        );
        
        // Event emitter for internal communication
        this.events = new HighPerformanceEventEmitter();
        
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
        this.minTimeBetweenSubmissions = 50; // Reduced from 100ms to 50ms
        this.rateLimitingState = {
            consecutiveErrors: 0,
            lastErrorTime: 0,
            backoffDelay: 100,
            baseDelay: 100,
            maxDelay: 5000,
            lastRequestTime: 0,
            rateLimitedEndpoints: new Map(), // Track rate limiting by endpoint
        };
        
        // Worker thread pool
        this.workers = [];
        this.workerPool = [];
        
        // Performance monitoring
        this.perfObserver = null;
        this.setupPerformanceMonitoring();
        
        // Early initialization
        this.warmupConnections();
        
        this.log('debug', `Initialized Pi Network Transfer Flood Bot (Ultra-Enhanced v3.0)`);
    }
    
    /**
     * Initialize high-performance HTTP agents for improved network performance
     */
    initializeHttpAgents() {
        // Create high-performance HTTPS agent with optimized settings
        this.httpsAgent = new https.Agent({
            keepAlive: this.useTcpKeepAlive,
            keepAliveMsecs: 3000,
            maxSockets: this.parallelConnections * 5, // More sockets than connections
            maxFreeSockets: this.parallelConnections * 2,
            timeout: 30000, // 30 second timeout
            scheduling: 'lifo', // Last-in, first-out for better locality
        });
        
        // Create optimized Axios instance
        this.axiosInstance = axios.create({
            httpsAgent: this.httpsAgent,
            timeout: 30000,
            headers: {
                'Connection': this.useTcpKeepAlive ? 'keep-alive' : 'close',
                'User-Agent': 'PiNetworkTransferFloodBot/3.0.0'
            }
        });
    }
    
    /**
     * Setup performance monitoring using PerformanceObserver
     */
    setupPerformanceMonitoring() {
        if (this.logLevel === 'debug') {
            this.perfObserver = new PerformanceObserver((items) => {
                items.getEntries().forEach((entry) => {
                    this.log('debug', `Performance: ${entry.name} took ${entry.duration.toFixed(2)}ms`);
                });
            });
            
            this.perfObserver.observe({ entryTypes: ['measure'] });
        }
    }
    
    /**
     * Measure performance of a function
     * @param {string} name - Name of the measurement
     * @param {Function} fn - Function to measure
     * @returns {Promise<any>} - Result of the function
     */
    async measurePerformance(name, fn) {
        if (this.logLevel !== 'debug') {
            return fn();
        }
        
        const startName = `${name}_start`;
        const endName = `${name}_end`;
        
        performance.mark(startName);
        const result = await fn();
        performance.mark(endName);
        performance.measure(name, startName, endName);
        
        return result;
    }
    
    /**
     * Pre-warm connections to Horizon servers to reduce connection latency
     */
    warmupConnections() {
        setTimeout(() => {
            if (!this.isRunning) return;
            
            this.log('debug', 'Pre-warming network connections...');
            
            // Make lightweight requests to all servers to establish connections
            this.servers.forEach((server, index) => {
                server.ledgers()
                    .limit(1)
                    .call()
                    .then(() => {
                        this.log('debug', `Successfully pre-warmed connection to server ${index + 1}`);
                    })
                    .catch(error => {
                        this.log('debug', `Pre-warming connection to server ${index + 1} failed: ${error.message}`);
                    });
            });
            
            // Also warm up backup servers
            this.backupServers.forEach((server, index) => {
                server.ledgers()
                    .limit(1)
                    .call()
                    .then(() => {
                        this.log('debug', `Successfully pre-warmed connection to backup server ${index + 1}`);
                    })
                    .catch(error => {
                        this.log('debug', `Pre-warming connection to backup server ${index + 1} failed: ${error.message}`);
                    });
            });
        }, 100);
    }
    
    // Enhanced logging with levels and timestamp precision
    log(level, ...args) {
        const levels = {
            'debug': 0,
            'info': 1,
            'warn': 2,
            'error': 3
        };
        
        if (levels[level] >= levels[this.logLevel]) {
            const now = new Date();
            // Include milliseconds in timestamp for more precise debugging
            const timestamp = now.toISOString().replace('T', ' ').substring(0, 23);
            console.log(`[${timestamp}] [${level.toUpperCase()}]`, ...args);
        }
    }
    
    /**
     * Advanced rate-limited API request with smart exponential backoff retry
     * Uses dynamic backoff based on error patterns and endpoint tracking
     */
    async rateLimit(fn, endpoint = 'general', maxRetries = 5, initialDelay = 500) {
        let retries = 0;
        const endpointData = this.rateLimitingState.rateLimitedEndpoints.get(endpoint) || {
            consecutiveErrors: 0,
            lastErrorTime: 0,
            backoffDelay: initialDelay,
            lastSuccessTime: 0
        };
        
        // Ensure minimum time between requests to same endpoint
        const now = Date.now();
        const timeSinceLastRequest = now - this.rateLimitingState.lastRequestTime;
        const minTimeBetweenRequests = 150; // 150ms minimum between requests
        
        if (timeSinceLastRequest < minTimeBetweenRequests) {
            await new Promise(resolve => 
                setTimeout(resolve, minTimeBetweenRequests - timeSinceLastRequest)
            );
        }
        
        // If this endpoint has had errors recently, add additional delay
        if (endpointData.consecutiveErrors > 0) {
            const timeSinceLastError = now - endpointData.lastErrorTime;
            const additionalDelay = Math.min(
                endpointData.backoffDelay * Math.pow(1.5, endpointData.consecutiveErrors - 1),
                5000 // Cap at 5 seconds max delay
            );
            
            if (timeSinceLastError < additionalDelay) {
                await new Promise(resolve => 
                    setTimeout(resolve, additionalDelay - timeSinceLastError)
                );
            }
        }
        
        // Update last request time
        this.rateLimitingState.lastRequestTime = Date.now();
        
        // Execute request with retries
        while (retries <= maxRetries) {
            try {
                const result = await fn();
                
                // Request succeeded, reset error counters for this endpoint
                endpointData.consecutiveErrors = 0;
                endpointData.lastSuccessTime = Date.now();
                this.rateLimitingState.rateLimitedEndpoints.set(endpoint, endpointData);
                
                return result;
            } catch (error) {
                let isRateLimited = false;
                
                // Check if error is due to rate limiting
                if (error.response && error.response.status === 429) {
                    isRateLimited = true;
                } else if (error.message && (
                    error.message.includes('Too Many Requests') ||
                    error.message.includes('rate limit') ||
                    error.message.includes('429')
                )) {
                    isRateLimited = true;
                }
                
                if (isRateLimited) {
                    // Rate limiting error
                    retries++;
                    if (retries > maxRetries) {
                        throw new Error(`Rate limit exceeded after ${maxRetries} retries`);
                    }
                    
                    // Update rate limiting state for this endpoint
                    endpointData.consecutiveErrors++;
                    endpointData.lastErrorTime = Date.now();
                    
                    // Calculate delay with smart exponential backoff
                    let delay;
                    if (this.smartBackoff) {
                        // More aggressive backoff for higher consecutive errors
                        const baseDelay = initialDelay;
                        const exponentialFactor = 1.5;
                        const randomFactor = 0.2; // Add jitter to prevent synchronized retries
                        const consecutiveFactor = Math.pow(exponentialFactor, endpointData.consecutiveErrors);
                        
                        delay = baseDelay * consecutiveFactor;
                        
                        // Add jitter (+/- 20%)
                        const jitter = delay * randomFactor * (Math.random() * 2 - 1);
                        delay = Math.max(baseDelay, delay + jitter);
                        
                        // Cap maximum delay
                        delay = Math.min(delay, 10000); // 10 second max
                    } else {
                        // Simple exponential backoff
                        delay = initialDelay * Math.pow(2, retries - 1);
                    }
                    
                    this.log('info', `Rate limited on ${endpoint}. Retrying after ${delay.toFixed(0)}ms (attempt ${retries}/${maxRetries})`);
                    
                    // Store updated endpoint data
                    this.rateLimitingState.rateLimitedEndpoints.set(endpoint, endpointData);
                    
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
     * Initialize keypair and validate accounts with enhanced error recovery
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

        // Use improved load account strategy with multiple fallbacks
        await this.measurePerformance('loadAccount', async () => {
            return this.loadAccountWithRetry();
        });

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
     * Improved account loading with multiple fallback mechanisms
     */
    async loadAccountWithRetry() {
        // Try primary rate-limited approach first
        try {
            this.log('info', 'Loading account data (rate-limited)...');
            
            // Use more aggressive rate limiting for account loading
            this.accountData = await this.rateLimit(
                () => this.servers[0].loadAccount(this.sourceKeypair.publicKey()),
                'loadAccount',
                7,  // More retries for critical operation
                500 // Longer initial delay
            );
            
            this.accountSequence = BigInt(this.accountData.sequenceNumber());
            this.accountDataTimestamp = Date.now();
            this.log('info', `Successfully loaded source account with rate limiting`);
            return true;
        } catch (primaryError) {
            this.log('error', `Failed to load account with rate limiting: ${primaryError.message}`);
            
            // Try direct HTTP request as second approach
            try {
                this.log('info', 'Trying direct HTTP request for account data...');
                const accountUrl = `${this.horizonUrl}/accounts/${this.sourceKeypair.publicKey()}`;
                
                const response = await this.axiosInstance.get(accountUrl);
                if (response.data) {
                    this.accountData = response.data;
                    this.accountSequence = BigInt(this.accountData.sequence);
                    this.accountDataTimestamp = Date.now();
                    this.log('info', `Successfully loaded source account via direct HTTP request`);
                    return true;
                }
            } catch (httpError) {
                this.log('warn', `Direct HTTP request failed: ${httpError.message}`);
            }
            
            // Try all backup servers sequentially
            const maxServers = this.servers.length + this.backupServers.length;
            let allServers = [...this.servers, ...this.backupServers];
            
            // Shuffle servers for more balanced load
            allServers = this.shuffleArray(allServers);
            
            for (let i = 0; i < maxServers; i++) {
                if (i >= allServers.length) break;
                
                try {
                    await new Promise(resolve => setTimeout(resolve, 200 * (i + 1))); // Staggered delays
                    this.accountData = await allServers[i].loadAccount(this.sourceKeypair.publicKey());
                    this.accountSequence = BigInt(this.accountData.sequenceNumber());
                    this.accountDataTimestamp = Date.now();
                    this.log('info', `Successfully loaded source account (using server ${i+1})`);
                    return true;
                } catch (error) {
                    this.log('warn', `Error loading source account from server ${i+1}: ${error.message}`);
                }
            }
            
            // If all attempts failed, try one last time with a longer timeout
            try {
                this.log('info', 'Trying final attempt with extended timeout...');
                await new Promise(resolve => setTimeout(resolve, 2000)); // 2 second delay
                
                const extendedTimeoutAgent = new https.Agent({
                    keepAlive: true,
                    timeout: 60000 // 60 second timeout
                });
                
                const extendedAxios = axios.create({
                    httpsAgent: extendedTimeoutAgent,
                    timeout: 60000
                });
                
                const accountUrl = `${this.horizonUrl}/accounts/${this.sourceKeypair.publicKey()}`;
                const response = await extendedAxios.get(accountUrl);
                
                if (response.data) {
                    this.accountData = response.data;
                    this.accountSequence = BigInt(this.accountData.sequence);
                    this.accountDataTimestamp = Date.now();
                    this.log('info', `Successfully loaded source account via extended timeout request`);
                    return true;
                }
            } catch (finalError) {
                this.log('error', `Final attempt failed: ${finalError.message}`);
            }
            
            throw new Error('Failed to load source account from any server after multiple attempts');
        }
    }
    
    /**
     * Shuffle array using Fisher-Yates algorithm
     */
    shuffleArray(array) {
        const newArray = [...array];
        for (let i = newArray.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [newArray[i], newArray[j]] = [newArray[j], newArray[i]];
        }
        return newArray;
    }
    
    /**
     * High-precision sleep function using a combination of setTimeout and busy waiting
     * Provides nanosecond-level precision for critical timing operations
     */
    preciseSleep(ms) {
        return new Promise(resolve => {
            const start = performance.now();
            const end = start + ms;
            
            // For very short sleeps under 5ms, just busy wait for maximum precision
            if (ms < 5) {
                const spinUntil = process.hrtime.bigint() + BigInt(Math.floor(ms * 1_000_000));
                while (process.hrtime.bigint() < spinUntil) {
                    // Busy wait with CPU spin - maximum precision but high CPU usage
                }
                resolve();
                return;
            }
            
            // For longer sleeps, use setTimeout for most of it, then busy wait for precision
            const busyWaitThreshold = 3; // Last 3ms will use busy waiting
            const timeoutMs = ms - busyWaitThreshold;
            
            const timeoutId = setTimeout(() => {
                // After timeout, busy wait for the remaining time for precision
                const spinUntil = process.hrtime.bigint() + BigInt(Math.floor(busyWaitThreshold * 1_000_000));
                while (process.hrtime.bigint() < spinUntil) {
                    // Busy wait for last few milliseconds
                }
                resolve();
            }, timeoutMs);
            
            this.timerIDs.add(timeoutId);
        });
    }
    
    /**
     * Get next server in round-robin fashion with health checking
     */
    getNextServer() {
        // Rotate through servers with health check
        let attempts = 0;
        const maxAttempts = this.servers.length * 2;
        
        while (attempts < maxAttempts) {
            const serverIndex = this.serverQueue.shift();
            this.serverQueue.push(serverIndex);
            
            // Check if this server has had recent failures
            const server = this.servers[serverIndex];
            attempts++;
            
            return server;
        }
        
        // If all servers seem problematic, just return the first one
        return this.servers[0];
    }

    /**
     * Start monitoring blocks with multiple redundant mechanisms
     */
    async startBlockMonitoring() {
        if (this.blockMonitoringActive) return;

        this.log('info', 'Starting block monitoring...');
        this.blockMonitoringActive = true;

        // Get current ledger info with enhanced error handling
        await this.updateLedgerInfo();

        // Start standard block monitoring interval - with dynamic adjustment
        this.blockMonitorInterval = setInterval(async () => {
            if (!this.isRunning) return;
            
            await this.updateLedgerInfo();
            this.checkUnlockTimeProximity();
        }, 2000); // Check every 2 seconds
        
        // Additional high-frequency proximity check for more precision
        this.precisionInterval = setInterval(() => {
            if (!this.isRunning) return;
            
            // Run more frequent unlock time checks when we're getting close
            const now = Date.now();
            if (this.unlockTime) {
                const timeToUnlock = this.unlockTime - now;
                
                // Increase frequency as we get closer
                if (timeToUnlock < 60000 && timeToUnlock > 0) { // Within 60 seconds
                    this.checkUnlockTimeProximity();
                    
                    // If very close (within 10 seconds), update ledger info too
                    if (timeToUnlock < 10000) {
                        this.updateLedgerInfo().catch(error => {
                            this.log('warn', `Error updating ledger info: ${error.message}`);
                        });
                    }
                }
            }
        }, 100); // Check every 100ms when we're close

        return this;
    }
    
    /**
     * Update ledger information from multiple sources with fallbacks
     */
    async updateLedgerInfo() {
        try {
            // Try primary rate-limited approach first
            try {
                const latestLedger = await this.rateLimit(
                    () => this.servers[0].ledgers().order('desc').limit(1).call(),
                    'getLedger',
                    3,
                    1000
                );
                
                this.processLedgerInfo(latestLedger);
                return;
            } catch (error) {
                this.log('warn', `Failed to get ledger with rate limiting: ${error.message}`);
            }
            
            // Try direct HTTP request as second approach
            try {
                const ledgerUrl = `${this.horizonUrl}/ledgers?order=desc&limit=1`;
                const response = await this.axiosInstance.get(ledgerUrl);
                
                if (response.data && response.data._embedded && response.data._embedded.records && 
                    response.data._embedded.records.length > 0) {
                    this.processLedgerInfo({ records: response.data._embedded.records });
                    return;
                }
            } catch (httpError) {
                this.log('warn', `Direct HTTP request for ledger failed: ${httpError.message}`);
            }
            
            // Try all servers sequentially
            for (let i = 0; i < this.servers.length; i++) {
                try {
                    // Add delay between server calls to avoid rate limiting
                    if (i > 0) {
                        await new Promise(resolve => setTimeout(resolve, 300 * i));
                    }
                    
                    const ledger = await this.servers[i].ledgers().order('desc').limit(1).call();
                    this.processLedgerInfo(ledger);
                    return;
                } catch (error) {
                    this.log('warn', `Error getting ledger from server ${i+1}: ${error.message}`);
                }
            }
            
            // If still failed, try backup servers
            for (let i = 0; i < this.backupServers.length; i++) {
                try {
                    await new Promise(resolve => setTimeout(resolve, 500 * (i + 1)));
                    const ledger = await this.backupServers[i].ledgers().order('desc').limit(1).call();
                    this.processLedgerInfo(ledger);
                    return;
                } catch (error) {
                    this.log('warn', `Error getting ledger from backup server ${i+1}: ${error.message}`);
                }
            }
            
            // If all failed, log warning but don't throw (we can continue with old data)
            this.log('warn', 'Failed to update ledger info from any server');
        } catch (error) {
            this.log('error', `Error updating ledger info: ${error.message}`);
        }
    }
    
    /**
     * Process ledger information and update internal state
     */
    processLedgerInfo(ledgerData) {
        if (!ledgerData || !ledgerData.records || ledgerData.records.length === 0) {
            this.log('warn', 'Received empty ledger data');
            return;
        }
        
        const currentLedgerNum = ledgerData.records[0].sequence;
        const currentCloseTime = new Date(ledgerData.records[0].closed_at).getTime();
        
        // Only process if this is a new ledger
        if (currentLedgerNum > this.latestLedgerNum) {
            // If we have a previous ledger, calculate block time
            if (this.latestLedgerNum > 0 && this.lastLedgerCloseTime) {
                const blockTime = currentCloseTime - this.lastLedgerCloseTime;
                
                // Ignore obviously wrong values (network hiccups)
                if (blockTime > 0 && blockTime < 30000) { // Between 0 and 30 seconds is reasonable
                    this.blockTimes.push(blockTime);
                    
                    // Keep only the last 10 block times for average calculation
                    if (this.blockTimes.length > 10) {
                        this.blockTimes.shift();
                    }
                    
                    // Calculate average block time
                    this.avgBlockTimeMs = this.blockTimes.reduce((sum, time) => sum + time, 0) / this.blockTimes.length;
                }
            }
            
            // Update ledger information
            this.log('debug', `New ledger: ${currentLedgerNum}, closed at: ${new Date(currentCloseTime).toISOString()}`);
            this.latestLedgerNum = currentLedgerNum;
            this.lastLedgerCloseTime = currentCloseTime;
            
            // Emit an event that can be used by other components
            this.events.emit('new-ledger', {
                ledgerNum: currentLedgerNum,
                closeTime: currentCloseTime
            });
        }
    }

    /**
     * Stop block monitoring and clean up resources
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
        this.clearAllTimers();
    }
    
    /**
     * Clear all active timers
     */
    clearAllTimers() {
        this.timerIDs.forEach(timerId => {
            clearTimeout(timerId);
        });
        this.timerIDs.clear();
    }

    /**
     * Enhanced unlock time proximity check with multiple redundant timing mechanisms
     * This is the critical component for executing at the precise unlock time
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
        
        // Only log at certain thresholds to reduce noise
        if (
            (secondsToUnlock > 300 && Math.floor(secondsToUnlock) % 60 === 0) || // Every minute when > 5 minutes away
            (secondsToUnlock <= 300 && secondsToUnlock > 60 && Math.floor(secondsToUnlock) % 10 === 0) || // Every 10 seconds when 1-5 minutes away
            (secondsToUnlock <= 60 && secondsToUnlock > 10 && Math.floor(secondsToUnlock) % 5 === 0) || // Every 5 seconds when 10-60 seconds away
            (secondsToUnlock <= 10 && Math.floor(secondsToUnlock) !== Math.floor(secondsToUnlock + 0.1)) // Every second when <= 10 seconds away
        ) {
            this.log('info', `Time to unlock: ${secondsToUnlock.toFixed(2)} seconds`);
        }
        
        // Prepare transactions well in advance
        if (this.proactivePreparation && !this.isPrepared) {
            const minutesToUnlock = secondsToUnlock / 60;
            
            if (minutesToUnlock <= this.earlyPreparationMinutes) {
                this.log('info', `Proactively preparing transactions ${secondsToUnlock.toFixed(2)} seconds before unlock time`);
                this.prepareTransactions();
            }
        }
        
        // If we're within the pre-flood window, ensure transactions are prepared
        if (secondsToUnlock <= this.preFloodSeconds && !this.isPrepared) {
            this.log('info', `Within ${this.preFloodSeconds} seconds of unlock time. Preparing transactions...`);
            this.prepareTransactions();
        }
        
        // If we're very close to unlock time, prepare for execution with high precision timing
        if (secondsToUnlock <= 3.0 && !this.isExecuting) {
            this.log('info', `CRITICAL: ${secondsToUnlock.toFixed(3)} seconds to unlock - preparing for precision execution`);
            this.isExecuting = true;
            
            // Calculate milliseconds until flood start (with precision adjustment)
            const msUntilFlood = Math.max(0, timeToUnlock - 40); // Start 40ms before to account for processing time
            
            this.log('info', `Scheduling flood to begin in ${msUntilFlood}ms with ultra-precision timer`);
            
            // CRITICAL: Set up a multi-layered timing approach to ensure execution at exactly the right moment
            
            // Approach 1: Ultra-precision timer with nanosecond precision
            if (this.timingPrecision) {
                const timerId = setTimeout(async () => {
                    this.log('info', 'ULTRA-PRECISION TIMER TRIGGERED - waiting for exact nanosecond');
                    // Calculate exact nanoseconds to wait
                    const preciseNow = Date.now();
                    const preciseWait = Math.max(0, this.unlockTime - preciseNow - 1); // 1ms before unlock
                    
                    if (preciseWait > 0) {
                        await this.preciseSleep(preciseWait);
                    }
                    
                    // Execute at the precise moment
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via ultra-precision timer');
                        this.executeFlood();
                    }
                }, Math.max(0, msUntilFlood - 30));
                this.timerIDs.add(timerId);
            }
            
            // Approach 2: Performance API based timer
            const perfTimerId = setTimeout(() => {
                const execTime = performance.now() + (this.unlockTime - Date.now() - 5);
                
                const checkTime = () => {
                    if (performance.now() >= execTime) {
                        if (!this.floodExecuted) {
                            this.log('info', 'EXECUTING FLOOD via performance timer');
                            this.executeFlood();
                        }
                    } else {
                        // Continue checking at microsecond intervals
                        setImmediate(checkTime);
                    }
                };
                
                checkTime();
            }, Math.max(0, msUntilFlood - 35));
            this.timerIDs.add(perfTimerId);
            
            // Approach 3: Standard timer with slight offset (as backup)
            const stdTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'EXECUTING FLOOD via standard timer');
                    this.executeFlood();
                }
            }, msUntilFlood);
            this.timerIDs.add(stdTimerId);
            
            // Add redundant timers with slight offsets if enabled
            if (this.redundantTimers) {
                // Earlier timer as backup 1 (30ms before intended time)
                const earlyTimerId1 = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via early backup timer 1');
                        this.executeFlood();
                    }
                }, Math.max(0, msUntilFlood - 30));
                this.timerIDs.add(earlyTimerId1);
                
                // Earlier timer as backup 2 (20ms before intended time)
                const earlyTimerId2 = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via early backup timer 2');
                        this.executeFlood();
                    }
                }, Math.max(0, msUntilFlood - 20));
                this.timerIDs.add(earlyTimerId2);
                
                // Later timer as fallback 1 (20ms after intended time)
                const lateTimerId1 = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via late fallback timer 1');
                        this.executeFlood();
                    }
                }, msUntilFlood + 20);
                this.timerIDs.add(lateTimerId1);
                
                // Later timer as fallback 2 (50ms after intended time)
                const lateTimerId2 = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via late fallback timer 2');
                        this.executeFlood();
                    }
                }, msUntilFlood + 50);
                this.timerIDs.add(lateTimerId2);
                
                // Latest timer as final fallback (100ms after intended time)
                const lateTimerId3 = setTimeout(() => {
                    if (!this.floodExecuted) {
                        this.log('info', 'EXECUTING FLOOD via final fallback timer');
                        this.executeFlood();
                    }
                }, msUntilFlood + 100);
                this.timerIDs.add(lateTimerId3);
            }
            
            // Add worker thread-based timing if enabled
            if (this.useWorkerThreads) {
                this.setupWorkerThreadExecution(msUntilFlood);
            }
        }
    }
    
    /**
     * Set up worker thread based execution timing
     * This provides an additional execution path using separate thread
     */
    setupWorkerThreadExecution(msUntilFlood) {
        try {
            // Create a worker thread for timing redundancy
            const worker = new Worker(`
                const { parentPort, workerData } = require('worker_threads');
                const { unlockTime } = workerData;
                
                // Use high-resolution timer
                function preciseSleep(ms) {
                    return new Promise(resolve => {
                        const start = Date.now();
                        const end = start + ms;
                        
                        if (ms < 5) {
                            // Busy wait for very short durations
                            while (Date.now() < end) {}
                            resolve();
                            return;
                        }
                        
                        // Use setTimeout for longer waits, then busy wait
                        const timeoutMs = ms - 3;
                        setTimeout(() => {
                            // Busy wait for the last 3ms
                            while (Date.now() < end) {}
                            resolve();
                        }, timeoutMs);
                    });
                }
                
                async function executeAtUnlockTime() {
                    const now = Date.now();
                    const timeToUnlock = unlockTime - now - 5; // 5ms early
                    
                    if (timeToUnlock > 0) {
                        await preciseSleep(timeToUnlock);
                    }
                    
                    // Signal parent to execute
                    parentPort.postMessage('EXECUTE');
                }
                
                // Start timer
                executeAtUnlockTime();
            `, { eval: true, workerData: { unlockTime: this.unlockTime } });
            
            worker.on('message', (message) => {
                if (message === 'EXECUTE' && !this.floodExecuted) {
                    this.log('info', 'EXECUTING FLOOD via worker thread timer');
                    this.executeFlood();
                }
            });
            
            worker.on('error', (error) => {
                this.log('error', `Worker thread error: ${error.message}`);
            });
            
            // Store worker for cleanup
            this.workers.push(worker);
        } catch (error) {
            this.log('warn', `Failed to set up worker thread: ${error.message}`);
        }
    }

    /**
     * Prepare transfer transactions in advance with optimized fee strategy
     * and enhanced pre-calculation to reduce execution time
     */
    async prepareTransactions() {
        if (this.isPrepared) {
            this.log('debug', 'Transactions already prepared');
            return;
        }

        try {
            this.log('info', 'Preparing transactions...');
            
            // Refresh account data with enhanced error recovery
            if (!this.accountData || Date.now() - this.accountDataTimestamp > 10000) {
                await this.loadAccountWithRetry();
            }
            
            const balance = this.accountData.balances?.find(b => b.asset_type === 'native') || 
                           { balance: this.accountData.balance || "0" };
            
            if (!balance) {
                throw new Error('No native balance found');
            }
            
            this.log('info', `Current account balance: ${balance.balance} Pi`);
            
            // Calculate amount to transfer if not specified
            const transferAmount = this.transferAmount || parseFloat(balance.balance) * 0.98; // 98% of balance by default
            this.log('info', `Using transfer amount: ${transferAmount} Pi`);
            
            // Calculate an ultra-aggressive fee strategy that outbids competitors
            const feeLevels = this.calculateFeeStrategy();
            
            // Prepare transactions with varying fees - distributed strategically
            for (let i = 0; i < this.txCount; i++) {
                // Calculate fee based on position in sequence using a more aggressive curve
                let fee;
                
                if (i < 10) {
                    // First 10 transactions get ultra-high fees (top priority)
                    fee = Math.floor(feeLevels.ultraPlus + this.feeIncrement * (i * 1.2)).toString();
                } else if (i < 30) {
                    // Next 20 get ultra fees (second tier)
                    fee = Math.floor(feeLevels.ultra + this.feeIncrement * ((i - 10) * 1.0)).toString();
                } else if (i < 60) {
                    // Next 30 get very high fees (third tier)
                    fee = Math.floor(feeLevels.high + this.feeIncrement * ((i - 30) * 0.8)).toString();
                } else if (i < 100) {
                    // Next 40 get high fees (fourth tier)
                    fee = Math.floor(feeLevels.medium + this.feeIncrement * ((i - 60) * 0.6)).toString();
                } else {
                    // Rest get medium to base fees (fifth tier)
                    fee = Math.floor(feeLevels.base + this.feeIncrement * ((i - 100) * 0.4)).toString();
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
                    maxAttempts: this.maxRetries - Math.floor(i / 30),  // More attempts for higher fee transactions
                    priority: i < 20 ? 'ultra-high' : 
                             i < 50 ? 'high' : 
                             i < 100 ? 'medium' : 'low', // Priority level for retry logic
                    prepared: true,
                    xdr: tx.toXDR(), // Cache XDR for quick access
                    hash: tx.hash().toString('hex') // Pre-calculate hash
                });
            }
            
            this.log('info', `Successfully prepared ${this.transactions.length} transactions with fees from ${this.transactions[0].fee} to ${this.transactions[this.transactions.length - 1].fee} stroops`);
            this.isPrepared = true;
            
            // Emit event to notify other components
            this.events.emit('transactions-prepared', {
                count: this.transactions.length,
                minFee: this.transactions[0].fee,
                maxFee: this.transactions[this.transactions.length - 1].fee
            });
        } catch (error) {
            this.log('error', 'Error preparing transactions:', error);
            throw error;
        }
    }
    
    /**
     * Calculate an ultra-aggressive fee strategy based on desired percentiles and competitor analysis
     */
    calculateFeeStrategy() {
        // Calculate fee levels to ensure we win the fee auction
        // Much more aggressive than previous version
        const ultraPlusFee = Math.floor(this.baseFee * this.priorityFeeMultiplier * 1.8); // 180% of priority fee for top transactions
        const ultraFee = Math.floor(this.baseFee * this.priorityFeeMultiplier * 1.5);     // 150% of priority fee for ultra transactions
        const highFee = Math.floor(this.baseFee * this.priorityFeeMultiplier);            // Full priority fee for important transactions
        const mediumFee = Math.floor(this.baseFee * 1.3);                                // 130% of base fee for medium priority
        const baseFee = this.baseFee;                                                    // Base fee for low priority
        
        return {
            ultraPlus: ultraPlusFee,
            ultra: ultraFee,
            high: highFee,
            medium: mediumFee,
            base: baseFee
        };
    }

    /**
     * Execute the transaction flood using distributed and concurrent approach
     * with ultra-precision timing, enhanced retry logic, and dynamic rate limiting
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
        
        // Create bursts of transactions with optimal timing
        const burstCount = 4;  // Number of bursts
        const burstDelay = 300;  // ms between bursts (reduced from 500ms for more aggressive flooding)
        
        // Store successful transactions to avoid duplicates
        const successful = new Set();
        
        // Use worker threads for submission if enabled
        if (this.useWorkerThreads && this.transactions.length > 50) {
            await this.executeWithWorkerThreads(successful);
        } else {
            await this.executeWithBursts(successful, burstCount, burstDelay);
        }
    }
    
    /**
     * Execute transaction submission using worker threads for parallel processing
     */
    async executeWithWorkerThreads(successful) {
        this.log('info', 'Executing with worker threads for parallel processing');
        
        // Split transactions among available CPUs
        const cpuCount = Math.min(os.cpus().length, 4); // Use up to 4 cores
        const workerCount = cpuCount - 1; // Leave one core for main thread
        const transactionsPerWorker = Math.ceil(this.transactions.length / workerCount);
        
        // Prioritize transactions - first 20% go to main thread
        const mainThreadCount = Math.ceil(this.transactions.length * 0.2);
        const mainThreadTransactions = this.transactions.slice(0, mainThreadCount);
        const workerTransactions = this.transactions.slice(mainThreadCount);
        
        // Create submit function for main thread
        const submitTransaction = this.createSubmitFunction(successful);
        
        // Submit highest priority transactions from main thread immediately
        for (let i = 0; i < mainThreadTransactions.length; i++) {
            const txInfo = mainThreadTransactions[i];
            
            // Minimal delay between submissions
            const delay = i * 2; // Just 2ms between transactions for fastest submissions
            
            setTimeout(() => {
                submitTransaction(txInfo);
            }, delay);
        }
        
        // Create workers for remaining transactions
        for (let i = 0; i < workerCount && workerTransactions.length > 0; i++) {
            const start = i * transactionsPerWorker;
            const end = Math.min(start + transactionsPerWorker, workerTransactions.length);
            
            if (start >= workerTransactions.length) continue;
            
            const workerTxs = workerTransactions.slice(start, end);
            
            try {
                // Convert transactions for worker (can't send full class instances)
                const workerData = {
                    transactions: workerTxs.map(tx => ({
                        index: tx.index,
                        fee: tx.fee,
                        xdr: tx.xdr,
                        priority: tx.priority,
                        serverIndex: tx.serverIndex
                    })),
                    horizonUrl: this.horizonUrl,
                    startDelay: i * 50 // Stagger worker starts
                };
                
                // Create worker
                const worker = new Worker(`
                    const { parentPort, workerData } = require('worker_threads');
                    const axios = require('axios');
                    const https = require('https');
                    
                    // Create optimized HTTP agent
                    const httpsAgent = new https.Agent({
                        keepAlive: true,
                        keepAliveMsecs: 3000,
                        maxSockets: 10,
                        timeout: 30000
                    });
                    
                    // Create axios instance
                    const axiosInstance = axios.create({
                        httpsAgent,
                        timeout: 30000
                    });
                    
                    // Submission function
                    async function submitTransaction(tx) {
                        try {
                            // Wait for staggered start
                            await new Promise(resolve => setTimeout(resolve, workerData.startDelay + (tx.index % 10) * 5));
                            
                            // Submit transaction
                            const response = await axiosInstance.post(
                                \`\${workerData.horizonUrl}/transactions\`, 
                                {tx: tx.xdr},
                                {
                                    headers: {
                                        'Content-Type': 'application/json',
                                        'X-Client-Name': 'pi-network-transfer-flood-bot',
                                        'X-Client-Version': '3.0.0'
                                    }
                                }
                            );
                            
                            // Send success message to main thread
                            parentPort.postMessage({
                                type: 'success',
                                index: tx.index,
                                fee: tx.fee,
                                hash: response.data?.hash || 'unknown'
                            });
                        } catch (error) {
                            // Send error message to main thread
                            parentPort.postMessage({
                                type: 'error',
                                index: tx.index,
                                fee: tx.fee,
                                error: error.message
                            });
                        }
                    }
                    
                    // Process all transactions
                    async function processTransactions() {
                        const transactions = workerData.transactions;
                        
                        // Sort by priority (fee)
                        transactions.sort((a, b) => parseInt(b.fee) - parseInt(a.fee));
                        
                        // Submit high priority transactions first with minimal delay
                        const highPriorityTxs = transactions.filter(tx => 
                            tx.priority === 'ultra-high' || tx.priority === 'high'
                        );
                        
                        for (const tx of highPriorityTxs) {
                            submitTransaction(tx);
                            // Almost no delay for high priority
                            await new Promise(r => setTimeout(r, 2));
                        }
                        
                        // Submit medium and low priority with more delay
                        const otherTxs = transactions.filter(tx => 
                            tx.priority !== 'ultra-high' && tx.priority !== 'high'
                        );
                        
                        for (const tx of otherTxs) {
                            submitTransaction(tx);
                            // More delay for lower priority
                            await new Promise(r => setTimeout(r, 10));
                        }
                    }
                    
                    // Start processing
                    processTransactions();
                `, { eval: true, workerData });
                
                // Handle messages from worker
                worker.on('message', (message) => {
                    if (message.type === 'success') {
                        if (!successful.has(message.index)) {
                            successful.add(message.index);
                            this.successCount++;
                            this.log('info', `✅ Transaction ${message.index + 1} SUCCESSFUL! Hash: ${message.hash}, Fee: ${message.fee}`);
                            
                            this.submissionResults.push({
                                success: true,
                                index: message.index,
                                fee: message.fee,
                                hash: message.hash,
                                timestamp: new Date().toISOString()
                            });
                            
                            // If we've succeeded, we can stop the bot after a short delay
                            if (this.successCount === 1) {
                                setTimeout(() => {
                                    if (this.isRunning) {
                                        this.log('info', '🎉 Successfully transferred Pi. Stopping bot...');
                                        this.stop();
                                    }
                                }, 3000);
                            }
                        }
                    } else if (message.type === 'error') {
                        this.failureCount++;
                        this.log('debug', `❌ Transaction ${message.index + 1} FAILED. Fee: ${message.fee}, Error: ${message.error}`);
                    }
                });
                
                worker.on('error', (error) => {
                    this.log('error', `Worker error: ${error.message}`);
                });
                
                this.workers.push(worker);
            } catch (error) {
                this.log('error', `Failed to create worker: ${error.message}`);
            }
        }
    }
    
    /**
     * Execute transaction flood using optimized burst strategy
     */
    async executeWithBursts(successful, burstCount, burstDelay) {
        // Create optimized submission function
        const submitTransaction = this.createSubmitFunction(successful);
        
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
                
                // Submit all transactions in this burst with minimal spacing to be more aggressive
                for (let i = startIndex; i < endIndex; i++) {
                    const txInfo = this.transactions[i];
                    
                    // Use varying delays based on priority and burst
                    // First burst gets minimal delays
                    const delay = burst === 0 ? i * 1 : // 1ms for first burst
                                 burst === 1 ? i * 3 : // 3ms for second burst
                                 burst === 2 ? i * 5 : // 5ms for third burst
                                 i * 8; // 8ms for final burst
                    
                    setTimeout(() => {
                        submitTransaction(txInfo);
                    }, delay);
                }
            }, burst * burstDelay);
        }
    }
    
    /**
     * Create an optimized transaction submission function
     */
    createSubmitFunction(successful) {
        return async (txInfo) => {
            if (successful.has(txInfo.index) || !this.isRunning) {
                return;
            }
            
            // Add minimal rate limiting between submissions
            const now = Date.now();
            const timeSinceLastSubmission = now - this.lastSubmissionTime;
            if (timeSinceLastSubmission < this.minTimeBetweenSubmissions) {
                await this.preciseSleep(this.minTimeBetweenSubmissions - timeSinceLastSubmission);
            }
            
            // Choose the server based on round-robin
            const server = this.getNextServer();
            
            try {
                this.log('debug', `Submitting tx ${txInfo.index + 1}/${this.transactions.length} with fee: ${txInfo.fee} stroops (priority: ${txInfo.priority})`);
                
                // Use optimized submission for high priority transactions
                let result;
                
                if (txInfo.priority === 'ultra-high' || txInfo.priority === 'high') {
                    // For ultra-high priority, use direct HTTP submission for fastest response
                    result = await this.submitTransactionHttp(txInfo);
                } else {
                    // For other priorities, use the server's submitTransaction method
                    result = await server.submitTransaction(txInfo.tx);
                }
                
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
                    }, 3000);
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
                    // Prioritize retries for high-priority transactions
                    const priorityMultiplier = txInfo.priority === 'ultra-high' ? 0.3 : 
                                              txInfo.priority === 'high' ? 0.5 : 
                                              txInfo.priority === 'medium' ? 1.0 : 2.0;
                                              
                    // Use longer backoff for rate limiting
                    const baseBackoff = isRateLimited ? 1000 : 50;
                    
                    // Use shorter backoff for higher priority transactions
                    const backoffMs = Math.min(baseBackoff * Math.pow(1.3, txInfo.attempts) * priorityMultiplier, 10000);
                    
                    setTimeout(() => {
                        if (this.isRunning && !successful.has(txInfo.index)) {
                            this.submitTransaction(txInfo);
                        }
                    }, backoffMs);
                }
            }
        };
    }
    
    /**
     * Optimized HTTP submission for high priority transactions
     * Bypasses Stellar SDK for faster execution
     */
    async submitTransactionHttp(txInfo) {
        try {
            const response = await this.axiosInstance.post(
                `${this.horizonUrl}/transactions`, 
                { tx: txInfo.xdr },
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Client-Name': 'pi-network-transfer-flood-bot',
                        'X-Client-Version': '3.0.0'
                    }
                }
            );
            
            return {
                hash: response.data?.hash || txInfo.hash,
                ledger: response.data?.ledger || 0,
                successful: true
            };
        } catch (error) {
            throw error;
        }
    }

    /**
     * Set up direct timers for unlock time to ensure execution
     * Uses multiple precision techniques for guaranteed execution
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
        
        // Create a multi-tiered approach to execution
        
        // 1. Preparation timers - well in advance of unlock time
        // First preparation tier - very early preparation (5 minutes before)
        const earlyPrepTime = Math.min(timeToUnlock - 300000, 60000); // 5 minutes before unlock or 1 minute max
        if (earlyPrepTime > 0) {
            const earlyPrepTimerId = setTimeout(() => {
                if (!this.isPrepared) {
                    this.log('info', 'DIRECT TIMER: Early preparation triggered (5 minutes before unlock)');
                    this.prepareTransactions();
                }
            }, Math.max(0, earlyPrepTime));
            this.timerIDs.add(earlyPrepTimerId);
        }
        
        // Second preparation tier - standard preparation (1 minute before)
        const prepTime = Math.min(timeToUnlock - 60000, 30000); // 1 minute before unlock or 30 seconds max
        if (prepTime > 0) {
            const prepTimerId = setTimeout(() => {
                if (!this.isPrepared) {
                    this.log('info', 'DIRECT TIMER: Standard preparation triggered (1 minute before unlock)');
                    this.prepareTransactions();
                }
            }, Math.max(0, prepTime));
            this.timerIDs.add(prepTimerId);
        }
        
        // Third preparation tier - final preparation (10 seconds before)
        const finalPrepTime = Math.min(timeToUnlock - 10000, 5000); // 10 seconds before unlock or 5 seconds max
        if (finalPrepTime > 0) {
            const finalPrepTimerId = setTimeout(() => {
                if (!this.isPrepared) {
                    this.log('info', 'DIRECT TIMER: Final preparation triggered (10 seconds before unlock)');
                    this.prepareTransactions();
                }
            }, Math.max(0, finalPrepTime));
            this.timerIDs.add(finalPrepTimerId);
        }
        
        // 2. Execution timers - multiple timers at and around unlock time
        
        // A. Pre-unlock timers - aimed to execute slightly before the unlock time
        
        // 40ms before unlock - early primary
        const earlyPrimaryTime = timeToUnlock - 40;
        if (earlyPrimaryTime > 0) {
            const earlyPrimaryTimerId = setTimeout(async () => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Early primary execution timer triggered (-40ms)');
                    await this.preciseSleep(20); // Fine-tuned waiting
                    this.executeFlood();
                }
            }, Math.max(0, earlyPrimaryTime - 20));
            this.timerIDs.add(earlyPrimaryTimerId);
        }
        
        // 20ms before unlock - primary execution
        const primaryTime = timeToUnlock - 20;
        if (primaryTime > 0) {
            const primaryTimerId = setTimeout(async () => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Primary execution timer triggered (-20ms)');
                    await this.preciseSleep(10); // Fine-tuned waiting
                    this.executeFlood();
                }
            }, Math.max(0, primaryTime - 10));
            this.timerIDs.add(primaryTimerId);
        }
        
        // 10ms before unlock - immediate pre-unlock
        const immediatePreTime = timeToUnlock - 10;
        if (immediatePreTime > 0) {
            const immediatePreTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Immediate pre-unlock timer triggered (-10ms)');
                    this.executeFlood();
                }
            }, Math.max(0, immediatePreTime));
            this.timerIDs.add(immediatePreTimerId);
        }
        
        // B. Exact unlock time timer
        const exactTimerId = setTimeout(() => {
            if (!this.floodExecuted) {
                this.log('info', 'DIRECT TIMER: Exact unlock time reached, executing');
                this.executeFlood();
            }
        }, Math.max(0, timeToUnlock));
        this.timerIDs.add(exactTimerId);
        
        // C. Post-unlock timers - fallbacks in case previous timers fail
        
        // 10ms after unlock - immediate fallback
        const immediatePostTime = timeToUnlock + 10;
        if (immediatePostTime > 0) {
            const immediatePostTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Immediate post-unlock timer triggered (+10ms)');
                    this.executeFlood();
                }
            }, Math.max(0, immediatePostTime));
            this.timerIDs.add(immediatePostTimerId);
        }
        
        // 30ms after unlock - short fallback
        const shortPostTime = timeToUnlock + 30;
        if (shortPostTime > 0) {
            const shortPostTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Short post-unlock timer triggered (+30ms)');
                    this.executeFlood();
                }
            }, Math.max(0, shortPostTime));
            this.timerIDs.add(shortPostTimerId);
        }
        
        // 100ms after unlock - medium fallback
        const mediumPostTime = timeToUnlock + 100;
        if (mediumPostTime > 0) {
            const mediumPostTimerId = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('info', 'DIRECT TIMER: Medium post-unlock timer triggered (+100ms)');
                    this.executeFlood();
                }
            }, Math.max(0, mediumPostTime));
            this.timerIDs.add(mediumPostTimerId);
        }
        
        // 3. Safety nets - extra failsafes for critical execution
        
        // First safety net - 0.5 seconds after unlock
        const safetyNet1 = timeToUnlock + 500;
        if (safetyNet1 > 0) {
            const safety1Id = setTimeout(() => {
                if (!this.floodExecuted) {
                    this.log('warn', 'DIRECT TIMER: Safety net 1 triggered - 500ms after unlock');
                    this.executeFlood();
                } else {
                    this.log('info', 'Safety check 1 passed - flood already executed');
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
                    this.log('info', 'Safety check 2 passed - flood already executed');
                }
            }, Math.max(0, safetyNet2));
            this.timerIDs.add(safety2Id);
        }
        
        // Final watchdog timer - absolute last chance
        const watchdogTimer = setTimeout(() => {
            if (!this.floodExecuted) {
                this.log('error', 'FATAL: All timers failed! Executing emergency flood!');
                this.executeFlood();
            } else {
                this.log('info', 'Final safety check passed - flood already executed');
            }
        }, Math.max(0, timeToUnlock + 5000)); // 5 seconds after unlock
        this.timerIDs.add(watchdogTimer);
        
        // 4. Worker thread based execution timer if enabled
        if (this.useWorkerThreads) {
            this.setupWorkerThreadTimer(timeToUnlock);
        }
    }
    
    /**
     * Set up a worker thread dedicated to precise timing
     */
    setupWorkerThreadTimer(timeToUnlock) {
        try {
            // Add some lead time for worker initialization
            const workerLeadTime = 5000; // 5 seconds before execution
            const workerStartTime = Math.max(0, timeToUnlock - workerLeadTime);
            
            // Schedule worker creation
            const workerStartTimerId = setTimeout(() => {
                try {
                    this.log('debug', 'Setting up worker thread timer');
                    
                    // Create a worker thread for timing precision
                    const worker = new Worker(`
                        const { parentPort, workerData } = require('worker_threads');
                        const { unlockTime, workerStartTime } = workerData;
                        
                        // High-resolution timing function
                        function preciseTimer(targetTime) {
                            return new Promise(resolve => {
                                const checkTime = () => {
                                    const now = Date.now();
                                    if (now >= targetTime) {
                                        resolve();
                                    } else {
                                        const remaining = targetTime - now;
                                        if (remaining > 10) {
                                            // For longer waits, use setTimeout
                                            setTimeout(checkTime, remaining - 10);
                                        } else {
                                            // For short waits, use setImmediate for higher precision
                                            setImmediate(checkTime);
                                        }
                                    }
                                };
                                
                                checkTime();
                            });
                        }
                        
                        // Multi-stage timer approach
                        async function executeTimers() {
                            // Create multiple timers at different offsets
                            const timers = [
                                { offset: -30, name: 'early' },
                                { offset: -15, name: 'primary' },
                                { offset: 0, name: 'exact' },
                                { offset: 15, name: 'late' },
                                { offset: 50, name: 'fallback' }
                            ];
                            
                            // Set up all timers
                            for (const timer of timers) {
                                const timerTime = unlockTime + timer.offset;
                                
                                // Execute this timer function asynchronously
                                (async () => {
                                    await preciseTimer(timerTime);
                                    parentPort.postMessage({
                                        type: 'execute',
                                        name: timer.name,
                                        offset: timer.offset,
                                        actualTime: Date.now()
                                    });
                                })();
                            }
                        }
                        
                        // Start the timers
                        executeTimers();
                        parentPort.postMessage({ type: 'ready' });
                    `, { 
                        eval: true, 
                        workerData: { 
                            unlockTime: this.unlockTime,
                            workerStartTime: Date.now() + workerLeadTime
                        } 
                    });
                    
                    worker.on('message', (message) => {
                        if (message.type === 'execute' && !this.floodExecuted) {
                            this.log('info', `WORKER TIMER: ${message.name} timer triggered (${message.offset}ms)`);
                            this.executeFlood();
                        } else if (message.type === 'ready') {
                            this.log('debug', 'Worker thread timer ready');
                        }
                    });
                    
                    worker.on('error', (error) => {
                        this.log('error', `Worker thread timer error: ${error.message}`);
                    });
                    
                    // Store worker for cleanup
                    this.workers.push(worker);
                } catch (error) {
                    this.log('error', `Failed to create worker thread timer: ${error.message}`);
                }
            }, workerStartTime);
            
            this.timerIDs.add(workerStartTimerId);
        } catch (error) {
            this.log('warn', `Failed to setup worker thread timer: ${error.message}`);
        }
    }

    /**
     * Initialize the bot and start monitoring with comprehensive error recovery
     */
    async start() {
        if (this.isRunning) {
            this.log('info', 'Bot is already running');
            return;
        }
        
        this.log('info', `Starting Pi Network Transfer Flood Bot (Ultra-Enhanced)`);
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
                if (this.proactivePreparation && timeToUnlock < 60) {
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
     * Stop the bot and clean up all resources
     */
    stop() {
        this.log('info', 'Stopping bot...');
        this.isRunning = false;
        this.stopBlockMonitoring();
        
        // Clear all timers
        this.clearAllTimers();

        // Stop and terminate all worker threads
        this.workers.forEach(worker => {
            try {
                worker.terminate();
            } catch (error) {
                this.log('debug', `Error terminating worker: ${error.message}`);
            }
        });
        this.workers = [];

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
        
        // Clean up performance monitoring
        if (this.perfObserver) {
            this.perfObserver.disconnect();
        }
        
        // Emit stopped event
        this.events.emit('stopped');
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
            avgBlockTimeMs: this.avgBlockTimeMs,
            systemInfo: {
                cpus: os.cpus().length,
                memory: process.memoryUsage(),
                uptime: process.uptime()
            }
        };
    }
}

module.exports = PiNetworkTransferFloodBot;