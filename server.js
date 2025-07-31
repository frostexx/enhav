/**
 * Enhanced Pi Network Transfer Flood Bot Server
 * 
 * Optimized server implementation with improved error handling,
 * real-time status updates, and performance enhancements.
 */
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const StellarSdk = require('stellar-sdk');
const PiNetworkTransferFloodBot = require('./pi-transfer-flood');
const path = require('path');
const fs = require('fs');
const os = require('os');

// Initialize Express app
const app = express();
const server = http.createServer(app);
const io = socketIO(server);

// Performance optimization: increase max listeners to avoid warnings
server.setMaxListeners(50);
process.setMaxListeners(50);

// Ensure public directory exists
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)){
    fs.mkdirSync(publicDir, { recursive: true });
    console.log('Created public directory');
}

// Serve static files from the public directory
app.use(express.static(publicDir));

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(publicDir, 'index.html'));
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'up',
        version: '2.0.0',
        timestamp: new Date().toISOString(),
        system: {
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            cpus: os.cpus().length,
            load: os.loadavg()
        }
    });
});

// Store active bots by socket ID
const activeBots = new Map();

// Bot activity monitoring
let botActivityInterval = null;

// Helper function to get locked balances for an account with enhanced error handling
async function getLockedBalances(publicKey) {
    try {
        // Create multiple server instances for redundancy
        const servers = [
            new StellarSdk.Server('https://api.mainnet.minepi.com'),
            new StellarSdk.Server('https://api.mainnet.minepi.com')
        ];
        
        let claimableBalances = null;
        let error = null;
        
        // Try multiple servers for redundancy
        for (const server of servers) {
            try {
                claimableBalances = await server
                    .claimableBalances()
                    .claimant(publicKey)
                    .limit(100)
                    .call();
                    
                if (claimableBalances && claimableBalances.records) {
                    break; // Success, exit the loop
                }
            } catch (err) {
                error = err;
                console.warn(`Failed to fetch claimable balances from server: ${err.message}`);
                continue; // Try the next server
            }
        }
        
        if (!claimableBalances) {
            throw error || new Error('Failed to fetch claimable balances from any server');
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
    } catch (error) {
        console.error('Error fetching locked balances:', error);
        return [];
    }
}

// Enhanced account loading with multiple server fallback
async function loadAccountWithFallback(publicKey) {
    const servers = [
        new StellarSdk.Server('https://api.mainnet.minepi.com'),
        new StellarSdk.Server('https://api.mainnet.minepi.com')
    ];
    
    let account = null;
    let lastError = null;
    
    for (const server of servers) {
        try {
            account = await server.loadAccount(publicKey);
            if (account) {
                return account;
            }
        } catch (error) {
            lastError = error;
            console.warn(`Failed to load account from server: ${error.message}`);
        }
    }
    
    throw lastError || new Error('Failed to load account from any server');
}

// Start bot activity monitoring
function startBotActivityMonitoring() {
    if (botActivityInterval) {
        clearInterval(botActivityInterval);
    }
    
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
                }
            } catch (error) {
                console.error(`Error monitoring bot for socket ${socketId}:`, error);
            }
        });
    }, 1000); // Check every second
}

// Stop bot activity monitoring
function stopBotActivityMonitoring() {
    if (botActivityInterval) {
        clearInterval(botActivityInterval);
        botActivityInterval = null;
    }
}

// Socket.IO connection handling
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    
    let bot = null;
    let publicKey = null;
    let passphrase = null;
    
    // Setup custom logging to socket
    const emitLog = (message, type = 'info') => {
        socket.emit('log', { message, type });
    };
    
    // Handle login
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
            const nativeBalance = account.balances.find(b => b.asset_type === 'native');
            const balance = nativeBalance ? nativeBalance.balance : '0';
            
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
            const nativeBalance = account.balances.find(b => b.asset_type === 'native');
            const balance = nativeBalance ? nativeBalance.balance : '0';
            
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
    
    // Handle get transaction history with improved error handling
    socket.on('get-txn-history', async () => {
        if (!publicKey) {
            emitLog('Not logged in', 'error');
            return;
        }
        
        try {
            emitLog('Fetching transaction history...');
            
            // Create multiple server instances for redundancy
            const servers = [
                new StellarSdk.Server('https://api.mainnet.minepi.com'),
                new StellarSdk.Server('https://api.mainnet.minepi.com')
            ];
            
            let transactions = null;
            
            // Try multiple servers for redundancy
            for (const server of servers) {
                try {
                    transactions = await server
                        .transactions()
                        .forAccount(publicKey)
                        .limit(20)
                        .order('desc')
                        .call();
                        
                    if (transactions && transactions.records) {
                        break; // Success, exit the loop
                    }
                } catch (err) {
                    console.warn(`Failed to fetch transactions from server: ${err.message}`);
                    continue; // Try the next server
                }
            }
            
            if (!transactions) {
                throw new Error('Failed to fetch transactions from any server');
            }
            
            // Process transactions
            const processedTxns = [];
            const server = servers[0]; // Use first server for operation details
            
            for (const txn of transactions.records) {
                try {
                    const operations = await server
                        .operations()
                        .forTransaction(txn.id)
                        .call();
                    
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
    
    // Handle start bot with enhanced configuration
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
            
            // Create new bot instance with enhanced configuration
            bot = new PiNetworkTransferFloodBot({
                sourcePassphrase: passphrase,
                targetAddress: data.targetAddress,
                transferAmount: data.transferAmount,
                baseFee: data.baseFee || 3500000,  // Increased default fee
                unlockTime: data.unlockTime,
                // Enhanced features
                timingPrecision: true,
                redundantTimers: true,
                proactivePreparation: true,
                logLevel: 'info',
                txCount: 100,  // Increased tx count
                parallelConnections: 10,  // More parallel connections
                preFloodSeconds: 5,  // Start preparing earlier
                priorityFeeMultiplier: 2.5  // More aggressive fee strategy
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
            
            // Start bot
            bot.start();
            
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

// Process termination handlers
process.on('SIGINT', () => {
    console.log('Received SIGINT. Shutting down server...');
    stopBotActivityMonitoring();
    
    // Stop all active bots
    activeBots.forEach((bot) => {
        bot.stop();
    });
    
    // Clear the activeBots map
    activeBots.clear();
    
    // Close the server
    server.close(() => {
        console.log('Server shutdown complete');
        process.exit(0);
    });
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM. Shutting down server...');
    stopBotActivityMonitoring();
    
    // Stop all active bots
    activeBots.forEach((bot) => {
        bot.stop();
    });
    
    // Clear the activeBots map
    activeBots.clear();
    
    // Close the server
    server.close(() => {
        console.log('Server shutdown complete');
        process.exit(0);
    });
});

// Uncaught exception handler
process.on('uncaughtException', (error) => {
    console.error('Uncaught exception:', error);
    
    // Log but don't exit - keep server running
    console.log('Server continuing despite error...');
});

// Start server
const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`Enhanced Pi Transfer Flood Bot Server running on port ${PORT}`);
    console.log(`Server time: ${new Date().toISOString()}`);
    console.log(`System: ${os.platform()} ${os.release()} (${os.cpus().length} CPUs, ${Math.round(os.totalmem() / 1024 / 1024 / 1024)} GB RAM)`);
    console.log('Ready to receive connections');
});