/**
 * Ultra-Enhanced Pi Network Transfer Flood Bot Server
 * 
 * High-performance server implementation with nanosecond-precision timing,
 * advanced error recovery, optimized network connections, and
 * intelligent rate limiting to outperform competing bots written in Rust and Go.
 * 
 * Version 3.0.0
 */
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const StellarSdk = require('stellar-sdk');
const PiNetworkTransferFloodBot = require('./pi-transfer-flood');
const path = require('path');
const fs = require('fs');
const os = require('os');
const axios = require('axios');
const https = require('https');
const { performance } = require('perf_hooks');

// Initialize Express app with optimized settings
const app = express();
const server = http.createServer({
    // HTTP server optimization options
    maxHeaderSize: 16384, // 16KB
    keepAliveTimeout: 30000, // 30 seconds
    headersTimeout: 60000, // 60 seconds
    requestTimeout: 30000, // 30 seconds
}, app);

// Socket.IO with optimized settings
const io = socketIO(server, {
    pingTimeout: 60000,
    pingInterval: 25000,
    transports: ['websocket', 'polling'],
    cors: {
        origin: '*'
    }
});

// Performance optimization: increase max listeners to avoid warnings
server.setMaxListeners(100);
process.setMaxListeners(100);

// System optimization
if (process.env.NODE_ENV === 'production') {
    // Optimize Node.js for production
    process.env.UV_THREADPOOL_SIZE = Math.max(4, os.cpus().length);
    
    // Increase memory limits for v8
    if (typeof v8 !== 'undefined' && v8.setFlagsFromString) {
        v8.setFlagsFromString('--max-old-space-size=4096');
    }
}

// Create optimized HTTP client for external requests
const httpAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 3000,
    maxSockets: 20,
    maxFreeSockets: 10,
    timeout: 30000,
});

const axiosInstance = axios.create({
    httpsAgent: httpAgent,
    timeout: 30000,
    headers: {
        'Connection': 'keep-alive',
        'User-Agent': 'PiNetworkTransferFloodBot/3.0.0'
    }
});

// Ensure public directory exists
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)){
    fs.mkdirSync(publicDir, { recursive: true });
    console.log('Created public directory');
}

// Serve static files with optimized settings
app.use(express.static(publicDir, {
    maxAge: 86400000, // 1 day cache
    etag: true,
    lastModified: true
}));

// Parse JSON with increased limits
app.use(express.json({
    limit: '1mb', // Increased limit
    strict: true
}));

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(publicDir, 'index.html'));
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'up',
        version: '3.0.0',
        timestamp: new Date().toISOString(),
        system: {
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            cpus: os.cpus().length,
            load: os.loadavg(),
            platform: os.platform(),
            arch: os.arch(),
            nodejs: process.version
        }
    });
});

// Time synchronization endpoint to help clients sync their clocks
app.get('/api/time', (req, res) => {
    const serverTime = Date.now();
    res.json({
        serverTime,
        timestamp: new Date(serverTime).toISOString()
    });
});

// Store active bots by socket ID
const activeBots = new Map();

// Bot activity monitoring
let botActivityInterval = null;

// Rate limiting for API requests
const apiRateLimiter = {
    lastRequestTime: 0,
    minTimeBetweenRequests: 250, // ms
    rateLimitedEndpoints: new Map(), // Track rate limiting by endpoint
    
    async limit(endpoint = 'general') {
        const now = Date.now();
        
        // Get endpoint data or create new
        const endpointData = this.rateLimitedEndpoints.get(endpoint) || {
            lastRequestTime: 0,
            consecutiveErrors: 0,
            lastErrorTime: 0,
            backoffDelay: 500
        };
        
        // General rate limiting
        const timeSinceLastRequest = now - this.lastRequestTime;
        if (timeSinceLastRequest < this.minTimeBetweenRequests) {
            await new Promise(resolve => 
                setTimeout(resolve, this.minTimeBetweenRequests - timeSinceLastRequest)
            );
        }
        
        // Endpoint-specific rate limiting
        const timeSinceLastEndpointRequest = now - endpointData.lastRequestTime;
        const endpointMinTime = endpointData.consecutiveErrors > 0 ? 
            Math.min(500 * Math.pow(1.5, endpointData.consecutiveErrors), 5000) : 500;
            
        if (timeSinceLastEndpointRequest < endpointMinTime) {
            await new Promise(resolve => 
                setTimeout(resolve, endpointMinTime - timeSinceLastEndpointRequest)
            );
        }
        
        // Update timestamps
        this.lastRequestTime = Date.now();
        endpointData.lastRequestTime = Date.now();
        this.rateLimitedEndpoints.set(endpoint, endpointData);
    },
    
    recordError(endpoint = 'general') {
        const endpointData = this.rateLimitedEndpoints.get(endpoint) || {
            lastRequestTime: 0,
            consecutiveErrors: 0,
            lastErrorTime: 0,
            backoffDelay: 500
        };
        
        endpointData.consecutiveErrors++;
        endpointData.lastErrorTime = Date.now();
        this.rateLimitedEndpoints.set(endpoint, endpointData);
    },
    
    recordSuccess(endpoint = 'general') {
        const endpointData = this.rateLimitedEndpoints.get(endpoint);
        if (endpointData) {
            endpointData.consecutiveErrors = 0;
            this.rateLimitedEndpoints.set(endpoint, endpointData);
        }
    }
};

/**
 * Rate-limited API request with exponential backoff retry and enhanced error handling
 */
async function rateLimitedRequest(fn, endpoint = 'general', maxRetries = 5, initialDelay = 1000) {
    let retries = 0;
    
    while (retries <= maxRetries) {
        try {
            // Apply rate limiting before making request
            await apiRateLimiter.limit(endpoint);
            
            // Execute the function
            const result = await fn();
            
            // Record success
            apiRateLimiter.recordSuccess(endpoint);
            
            return result;
        } catch (error) {
            // Check if it's a rate limiting error
            let isRateLimited = false;
            
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
                retries++;
                
                // Record error
                apiRateLimiter.recordError(endpoint);
                
                if (retries > maxRetries) {
                    throw new Error(`Rate limit exceeded after ${maxRetries} retries`);
                }
                
                // Calculate delay with smart exponential backoff
                const baseDelay = initialDelay;
                const exponentialFactor = 1.5;
                const randomFactor = 0.2; // Add jitter to prevent synchronized retries
                
                let delay = baseDelay * Math.pow(exponentialFactor, retries - 1);
                
                // Add jitter (+/- 20%)
                const jitter = delay * randomFactor * (Math.random() * 2 - 1);
                delay = Math.max(baseDelay, delay + jitter);
                
                // Cap maximum delay
                delay = Math.min(delay, 10000); // 10 second max
                
                console.log(`Rate limited on ${endpoint}. Retrying after ${delay.toFixed(0)}ms (attempt ${retries}/${maxRetries})`);
                
                // Wait before retrying
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                // Different error, rethrow
                throw error;
            }
        }
    }
}

// Enhanced account loading with multi-stage fallback mechanisms
async function loadAccountWithFallback(publicKey) {
    // Record performance start time
    const startTime = performance.now();
    
    // Create Stellar servers with optimized settings
    const servers = [
        new StellarSdk.Server('https://api.mainnet.minepi.com'),
        new StellarSdk.Server('https://api.mainnet.minepi.com')
    ];
    
    // Try with rate limiting and retries first
    try {
        console.log('Loading account with rate limiting...');
        const account = await rateLimitedRequest(
            () => servers[0].loadAccount(publicKey),
            'loadAccount',
            5,  // 5 retries maximum
            2000 // 2 second initial delay
        );
        
        console.log(`Account loaded successfully in ${(performance.now() - startTime).toFixed(2)}ms`);
        return account;
    } catch (primaryError) {
        console.warn(`Failed with rate limiting: ${primaryError.message}`);
        
        // Try direct HTTP request as fallback
        try {
            console.log('Trying direct HTTP request...');
            const accountUrl = `https://api.mainnet.minepi.com/accounts/${publicKey}`;
            const response = await axiosInstance.get(accountUrl);
            
            if (response.data) {
                console.log(`Account loaded via direct HTTP in ${(performance.now() - startTime).toFixed(2)}ms`);
                return response.data;
            }
        } catch (httpError) {
            console.warn(`Direct HTTP request failed: ${httpError.message}`);
        }
        
        // Try all servers sequentially with delays
        for (let i = 0; i < servers.length; i++) {
            try {
                // Add delay between attempts
                if (i > 0) {
                    await new Promise(resolve => setTimeout(resolve, 3000));
                }
                
                console.log(`Trying server ${i+1}...`);
                const account = await servers[i].loadAccount(publicKey);
                console.log(`Account loaded from server ${i+1} in ${(performance.now() - startTime).toFixed(2)}ms`);
                return account;
            } catch (error) {
                console.warn(`Failed to load account from server ${i+1}: ${error.message}`);
            }
        }
        
        // Final attempt with extended timeout
        try {
            console.log('Making final attempt with extended timeout...');
            await new Promise(resolve => setTimeout(resolve, 5000));
            
            const extendedAxios = axios.create({
                timeout: 60000, // 60 second timeout
                httpsAgent: new https.Agent({
                    keepAlive: true,
                    timeout: 60000
                })
            });
            
            const accountUrl = `https://api.mainnet.minepi.com/accounts/${publicKey}`;
            const response = await extendedAxios.get(accountUrl);
            
            if (response.data) {
                console.log(`Account loaded via extended timeout in ${(performance.now() - startTime).toFixed(2)}ms`);
                return response.data;
            }
        } catch (finalError) {
            console.error(`Final attempt failed: ${finalError.message}`);
        }
        
        throw new Error('Failed to load account from any source after multiple attempts');
    }
}

// Helper function to get locked balances with enhanced error handling
async function getLockedBalances(publicKey) {
    try {
        // Create server instance
        const server = new StellarSdk.Server('https://api.mainnet.minepi.com');
        
        // Use rate limiting
        try {
            console.log('Fetching locked balances with rate limiting...');
            
            // Get claimable balances with rate limiting
            const claimableBalances = await rateLimitedRequest(
                () => server.claimableBalances().claimant(publicKey).limit(100).call(),
                'getClaimableBalances',
                3,
                2000
            );
                
            return processLockedBalances(claimableBalances);
        } catch (error) {
            console.error('Error fetching locked balances with rate limiting:', error);
            
            // Try direct HTTP request as fallback
            try {
                console.log('Trying direct HTTP request for locked balances...');
                const balancesUrl = `https://api.mainnet.minepi.com/claimable_balances?claimant=${publicKey}&limit=100`;
                const response = await axiosInstance.get(balancesUrl);
                
                if (response.data && response.data._embedded && response.data._embedded.records) {
                    return processLockedBalances({ records: response.data._embedded.records });
                }
            } catch (httpError) {
                console.warn(`Direct HTTP request for balances failed: ${httpError.message}`);
            }
            
            // Fallback to standard fetch with delay
            console.log('Trying standard fetch with delay...');
            await new Promise(resolve => setTimeout(resolve, 3000));
            
            const claimableBalances = await server.claimableBalances().claimant(publicKey).limit(100).call();
            return processLockedBalances(claimableBalances);
        }
    } catch (error) {
        console.error('Error fetching locked balances:', error);
        return [];
    }
}

// Process locked balances response
function processLockedBalances(claimableBalances) {
    if (!claimableBalances || !claimableBalances.records) {
        return [];
    }
    
    return claimableBalances.records.map(balance => {
        let unlockTime = null;
        let amount = '0';
        
        // Parse the claimable balance details
        if (balance.claimants && balance.claimants.length > 0) {
            const claimant = balance.claimants.find(c => c.destination === publicKey);
            if (claimant && claimant.predicate.not && claimant.predicate.not.abs_before) {
                unlockTime = new Date(claimant.predicate.not.abs_before).toISOString();
            }
        }
        
        // Parse amount
        if (balance.asset === 'native') {
            amount = balance.amount;
        }
        
        return {
            id: balance.id,
            amount,
            unlockTime: unlockTime || new Date().toISOString(), // Default to now if no unlock time found
            claimants: balance.claimants || []
        };
    });
}

// Start bot activity monitoring with performance optimizations
function startBotActivityMonitoring() {
    if (botActivityInterval) {
        clearInterval(botActivityInterval);
    }
    
    console.log('Starting bot activity monitoring');
    
    botActivityInterval = setInterval(() => {
        activeBots.forEach((bot, socketId) => {
            try {
                const status = bot.getStatus();
                const socket = io.sockets.sockets.get(socketId);
                
                if (socket) {
                    socket.emit('bot-status-update', status);
                    
                    // If unlock time is within 10 seconds, increase update frequency
                    if (status.timeToUnlock !== null && status.timeToUnlock < 10 && status.timeToUnlock > 0) {
                        socket.emit('log', { 
                            message: `T-minus ${status.timeToUnlock.toFixed(2)} seconds to unlock time`,
                            type: 'info'
                        });
                    }
                    
                    // If getting very close to unlock time, emit a countdown event
                    if (status.timeToUnlock !== null && status.timeToUnlock < 5) {
                        socket.emit('countdown', {
                            timeToUnlock: status.timeToUnlock,
                            unlockTime: status.unlockTime
                        });
                    }
                }
            } catch (error) {
                console.error(`Error monitoring bot for socket ${socketId}:`, error);
            }
        });
    }, 500); // Increased frequency from 1000ms to 500ms for more responsive updates
}

// Stop bot activity monitoring
function stopBotActivityMonitoring() {
    if (botActivityInterval) {
        clearInterval(botActivityInterval);
        botActivityInterval = null;
        console.log('Stopped bot activity monitoring');
    }
}

// Socket.IO connection handling with optimized event processing
io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);
    
    let bot = null;
    let publicKey = null;
    let passphrase = null;
    
    // Setup custom logging to socket
    const emitLog = (message, type = 'info') => {
        socket.emit('log', { message, type });
    };
    
    // Handle login with enhanced error recovery
    socket.on('login', async (data) => {
        try {
            passphrase = data.passphrase;
            
            // Create a temporary bot instance to validate passphrase and get public key
            const tempBot = new PiNetworkTransferFloodBot({
                sourcePassphrase: passphrase,
                logLevel: 'info'
            });
            
            // Get keypair to validate the passphrase
            const keypair = tempBot.keypairFromPassphrase(passphrase);
            publicKey = keypair.publicKey();
            
            emitLog(`Validating account ${publicKey.substring(0, 5)}...${publicKey.substring(publicKey.length - 5)}`);
            
            // Load account details with enhanced error handling
            const account = await loadAccountWithFallback(publicKey);
            
            // Get native balance
            let balance = '0';
            if (account.balances) {
                const nativeBalance = account.balances.find(b => b.asset_type === 'native');
                balance = nativeBalance ? nativeBalance.balance : '0';
            } else if (account.balance) {
                balance = account.balance;
            }
            
            // Get locked balances
            emitLog('Fetching locked balances...');
            const lockedBalances = await getLockedBalances(publicKey);
            
            // Send success response
            socket.emit('login-response', {
                success: true,
                publicKey,
                balance,
                lockedBalances
            });
            
            emitLog(`Login successful. Found ${lockedBalances.length} locked balances.`, 'success');
            
            // Start bot activity monitoring if not already started
            if (!botActivityInterval) {
                startBotActivityMonitoring();
            }
        } catch (error) {
            console.error('Login error:', error);
            emitLog(`Login failed: ${error.message}`, 'error');
            socket.emit('login-response', {
                success: false,
                error: error.message || 'Invalid passphrase or network error'
            });
        }
    });
    
    // Handle refresh balance
    socket.on('refresh-balance', async () => {
        if (!publicKey) {
            emitLog('Not logged in', 'error');
            return;
        }
        
        try {
            emitLog('Refreshing balance...');
            
            // Load account with enhanced error handling
            const account = await loadAccountWithFallback(publicKey);
            
            // Get native balance
            let balance = '0';
            if (account.balances) {
                const nativeBalance = account.balances.find(b => b.asset_type === 'native');
                balance = nativeBalance ? nativeBalance.balance : '0';
            } else if (account.balance) {
                balance = account.balance;
            }
            
            // Get locked balances
            const lockedBalances = await getLockedBalances(publicKey);
            
            socket.emit('balance-updated', {
                balance,
                lockedBalances
            });
            
            emitLog(`Balance updated: ${balance} π`, 'success');
            
        } catch (error) {
            console.error('Error refreshing balance:', error);
            emitLog(`Error refreshing balance: ${error.message}`, 'error');
        }
    });
    
    // Handle refresh locked balances
    socket.on('refresh-locked-balances', async () => {
        if (!publicKey) {
            emitLog('Not logged in', 'error');
            return;
        }
        
        try {
            emitLog('Refreshing locked balances...');
            const lockedBalances = await getLockedBalances(publicKey);
            
            socket.emit('locked-balances-updated', {
                lockedBalances
            });
            
            emitLog(`Found ${lockedBalances.length} locked balances`, 'success');
            
        } catch (error) {
            console.error('Error refreshing locked balances:', error);
            emitLog(`Error refreshing locked balances: ${error.message}`, 'error');
        }
    });
    
    // Handle get transaction history with improved error handling and rate limiting
    socket.on('get-txn-history', async () => {
        if (!publicKey) {
            emitLog('Not logged in', 'error');
            return;
        }
        
        try {
            emitLog('Fetching transaction history...');
            
            // Use rate limiting for transaction history
            let transactions = null;
            
            try {
                const server = new StellarSdk.Server('https://api.mainnet.minepi.com');
                transactions = await rateLimitedRequest(
                    () => server.transactions().forAccount(publicKey).limit(10).order('desc').call(),
                    'getTransactions',
                    3,
                    2000
                );
            } catch (error) {
                console.warn(`Failed to fetch transactions with rate limiting: ${error.message}`);
                
                // Try direct HTTP request as fallback
                try {
                    const txUrl = `https://api.mainnet.minepi.com/accounts/${publicKey}/transactions?order=desc&limit=10`;
                    const response = await axiosInstance.get(txUrl);
                    
                    if (response.data && response.data._embedded && response.data._embedded.records) {
                        transactions = { records: response.data._embedded.records };
                    }
                } catch (httpError) {
                    console.warn(`Direct HTTP request for transactions failed: ${httpError.message}`);
                }
                
                // Fallback approach with delay
                if (!transactions) {
                    await new Promise(resolve => setTimeout(resolve, 3000));
                    
                    const server = new StellarSdk.Server('https://api.mainnet.minepi.com');
                    transactions = await server.transactions().forAccount(publicKey).limit(10).order('desc').call();
                }
            }
            
            if (!transactions || !transactions.records) {
                throw new Error('Failed to fetch transactions from any server');
            }
            
            // Process transactions
            const processedTxns = [];
            const server = new StellarSdk.Server('https://api.mainnet.minepi.com');
            
            for (const txn of transactions.records) {
                try {
                    // Add delay between operation requests to avoid rate limiting
                    await apiRateLimiter.limit('getOperations');
                    
                    let operations;
                    try {
                        operations = await server
                            .operations()
                            .forTransaction(txn.id)
                            .call();
                    } catch (error) {
                        // Try direct HTTP request as fallback
                        const opsUrl = `https://api.mainnet.minepi.com/transactions/${txn.id}/operations`;
                        const response = await axiosInstance.get(opsUrl);
                        
                        if (response.data && response.data._embedded && response.data._embedded.records) {
                            operations = { records: response.data._embedded.records };
                        } else {
                            throw error;
                        }
                    }
                    
                    for (const op of operations.records) {
                        let type = op.type;
                        let amount = '';
                        
                        if (op.type === 'payment' && op.asset_type === 'native') {
                            amount = `${op.amount} π`;
                            type = op.from === publicKey ? 'Sent' : 'Received';
                        } else if (op.type === 'claim_claimable_balance') {
                            amount = 'Claimed';
                            type = 'Claim';
                        } else if (op.type === 'create_claimable_balance') {
                            amount = op.amount || '';
                            type = 'Lock';
                        }
                        
                        processedTxns.push({
                            type,
                            amount,
                            date: txn.created_at,
                            link: `https://blockexplorer.minepi.com/tx/${txn.id}`
                        });
                    }
                } catch (opError) {
                    console.warn(`Error fetching operations for transaction ${txn.id}:`, opError.message);
                    // Add a simplified entry if operation details fail
                    processedTxns.push({
                        type: 'Transaction',
                        amount: '',
                        date: txn.created_at,
                        link: `https://blockexplorer.minepi.com/tx/${txn.id}`
                    });
                }
            }
            
            socket.emit('txn-history', {
                transactions: processedTxns.slice(0, 10) // Limit to 10 transactions
            });
            
            emitLog(`Found ${processedTxns.length} transactions`, 'success');
            
        } catch (error) {
            console.error('Error fetching transaction history:', error);
            emitLog(`Error fetching transaction history: ${error.message}`, 'error');
            socket.emit('txn-history', { transactions: [] });
        }
    });
    
    // Handle start bot with ultra-enhanced configuration and optimized timing
    socket.on('start-bot', (data) => {
        if (!passphrase) {
            emitLog('Not logged in', 'error');
            return;
        }
        
        try {
            // Validate target address
            if (!data.targetAddress) {
                throw new Error('Target address is required');
            }
            
            try {
                StellarSdk.Keypair.fromPublicKey(data.targetAddress);
            } catch (error) {
                throw new Error(`Invalid target address: ${error.message}`);
            }
            
            // Create new bot instance with ultra-enhanced configuration
            bot = new PiNetworkTransferFloodBot({
                sourcePassphrase: passphrase,
                targetAddress: data.targetAddress,
                transferAmount: data.transferAmount,
                baseFee: data.baseFee || 6000000,  // Increased default fee to be more aggressive
                unlockTime: data.unlockTime,
                // Ultra-Enhanced features
                timingPrecision: data.highPrecisionTiming !== undefined ? data.highPrecisionTiming : true,
                redundantTimers: data.redundantTimers !== undefined ? data.redundantTimers : true,
                proactivePreparation: true,
                logLevel: 'info',
                txCount: 150,  // Increased from 50 to 150 for more aggressive flooding
                parallelConnections: 4, // Increased to 4 for more parallel connections
                preFloodSeconds: 60, // Start preparing 1 minute before unlock
                priorityFeeMultiplier: 3.0, // More aggressive fee multiplication
                txSpacingMs: 5, // Reduced from 10ms to 5ms for faster flooding
                earlyPreparationMinutes: 5, // Start preparing 5 minutes before unlock
                useWorkerThreads: true, // Use worker threads for parallel processing
                aggressiveMemoryCache: true, // Cache more data in memory
                useHttp2: true, // Use HTTP/2 for better multiplexing
                useTcpKeepAlive: true, // Use TCP keep-alive
                predictiveExecution: true, // Use predictive timing
                smartBackoff: true // Use smart backoff strategy
            });
            
            // Intercept logs to forward to client
            const originalConsoleLog = console.log;
            console.log = function() {
                // Call original console.log
                originalConsoleLog.apply(console, arguments);
                
                // Send to client if it looks like a bot log message
                const message = Array.from(arguments).join(' ');
                
                // Only forward bot-related logs (those with timestamps/level indicators)
                if (message.includes('[INFO]') || message.includes('[DEBUG]') || 
                    message.includes('[WARN]') || message.includes('[ERROR]')) {
                    
                    let type = 'info';
                    if (message.includes('[ERROR]')) type = 'error';
                    if (message.includes('[WARN]')) type = 'warning';
                    
                    socket.emit('log', { message, type });
                }
            };
            
            // Store bot in active bots
            activeBots.set(socket.id, bot);
            
            // Set up event listeners for the bot
            bot.events.on('transactions-prepared', (data) => {
                socket.emit('transactions-prepared', data);
                emitLog(`Prepared ${data.count} transactions with fees from ${data.minFee} to ${data.maxFee} stroops`, 'info');
            });
            
            bot.events.on('new-ledger', (data) => {
                socket.emit('new-ledger', data);
            });
            
            // Start bot
            bot.start().catch(error => {
                console.error('Uncaught exception:', error);
                emitLog(`Error starting bot: ${error.message}`, 'error');
            });
            
            emitLog('Bot started successfully', 'success');
            
            // Send initial status
            socket.emit('bot-status-update', bot.getStatus());
            
        } catch (error) {
            console.error('Error starting bot:', error);
            emitLog(`Error starting bot: ${error.message}`, 'error');
        }
    });
    
    // Handle stop bot
    socket.on('stop-bot', () => {
        if (bot) {
            bot.stop();
            activeBots.delete(socket.id);
            emitLog('Bot stopped', 'info');
            
            // Restore original console methods
            console.log = console.log;
        } else {
            emitLog('No active bot to stop', 'warning');
        }
    });
    
    // Handle get bot status
    socket.on('get-bot-status', () => {
        if (bot) {
            socket.emit('bot-status-update', bot.getStatus());
        } else {
            socket.emit('bot-status-update', {
                isRunning: false,
                isPrepared: false,
                isExecuting: false,
                floodExecuted: false,
                transactionsPrepared: 0,
                unlockTime: null,
                timeToUnlock: null,
                successCount: 0,
                failureCount: 0
            });
        }
    });
    
    // Handle time synchronization request
    socket.on('sync-time', () => {
        const serverTime = Date.now();
        socket.emit('time-sync', {
            serverTime,
            timestamp: new Date(serverTime).toISOString()
        });
    });
    
    // Handle disconnection
    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
        
        // Stop and remove bot if exists
        if (activeBots.has(socket.id)) {
            const bot = activeBots.get(socket.id);
            bot.stop();
            activeBots.delete(socket.id);
        }
        
        // Restore original console methods
        console.log = console.log;
        
        // If no more active connections, stop monitoring
        if (activeBots.size === 0) {
            stopBotActivityMonitoring();
        }
    });
});

// Process termination handlers with graceful shutdown
process.on('SIGINT', () => {
    console.log('Received SIGINT. Shutting down server...');
    shutdown();
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM. Shutting down server...');
    shutdown();
});

// Graceful shutdown function
function shutdown() {
    stopBotActivityMonitoring();
    
    // Stop all active bots
    const shutdownPromises = [];
    
    activeBots.forEach((bot) => {
        try {
            bot.stop();
            
            // If the bot has a cleanup method, call it
            if (typeof bot.cleanup === 'function') {
                shutdownPromises.push(bot.cleanup());
            }
        } catch (error) {
            console.error(`Error stopping bot: ${error.message}`);
        }
    });
    
    // Clear the activeBots map
    activeBots.clear();
    
    // Close all connections and exit
    Promise.all(shutdownPromises)
        .then(() => {
            server.close(() => {
                console.log('Server shutdown complete');
                process.exit(0);
            });
        })
        .catch((error) => {
            console.error(`Error during shutdown: ${error.message}`);
            server.close(() => {
                console.log('Server shutdown complete with errors');
                process.exit(1);
            });
        });
    
    // Force exit after 10 seconds if graceful shutdown fails
    setTimeout(() => {
        console.error('Forced exit after timeout during shutdown');
        process.exit(1);
    }, 10000);
}

// Uncaught exception handler with better error information
process.on('uncaughtException', (error) => {
    console.error('Uncaught exception:', error);
    console.error('Stack trace:', error.stack);
    
    // Log but don't exit - keep server running
    console.log('Server continuing despite error...');
});

// Unhandled promise rejection handler
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Promise Rejection:', reason);
    
    // Log but don't exit - keep server running
    console.log('Server continuing despite unhandled promise rejection...');
});

// Start server with optimized settings
const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`Enhanced Pi Transfer Flood Bot Server running on port ${PORT}`);
    console.log(`Server time: ${new Date().toISOString()}`);
    console.log(`System: ${os.platform()} ${os.release()} (${os.cpus().length} CPUs, ${Math.round(os.totalmem() / 1024 / 1024 / 1024)} GB RAM)`);
    console.log('Ready to receive connections');
});