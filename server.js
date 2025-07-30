const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const StellarSdk = require('stellar-sdk');
const path = require('path');
const PiNetworkTransferFloodBot = require('./pi-transfer-flood-enhanced');
const axios = require('axios');
const { performance } = require('perf_hooks');

// Constants
const PORT = process.env.PORT || 3000;
const HORIZON_URL = 'https://api.mainnet.minepi.com';
const NETWORK_PASSPHRASE = 'Pi Network';
const HORIZON_URLS = [
    'https://api.mainnet.minepi.com',
    'https://api.minepi.com'
];

// Express setup
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// Create HTTP server
const server = http.createServer(app);

// Socket.io setup
const io = socketIo(server);

// Store active bots and passphrases
const activeBots = new Map();
const activePassphrases = new Map();

// Time synchronization service
let networkTimeOffset = 0;
let lastTimeSyncTime = 0;
const TIME_SYNC_INTERVAL = 60000; // 1 minute

// Synchronize time with Pi Network servers
async function syncNetworkTime() {
    try {
        const startTime = performance.now();
        const response = await axios.get(HORIZON_URL, { timeout: 3000 });
        const roundTripTime = performance.now() - startTime;
        
        // Get server time and account for round trip
        const serverTime = new Date(response.headers.date).getTime();
        const localTime = Date.now();
        
        // Adjust for round trip (approximating server time is halfway through the request)
        const adjustedServerTime = serverTime + (roundTripTime / 2);
        
        // Calculate offset
        networkTimeOffset = adjustedServerTime - localTime;
        
        lastTimeSyncTime = localTime;
        console.log(`Network time synchronized. Offset: ${networkTimeOffset}ms, RTT: ${roundTripTime}ms`);
    } catch (error) {
        console.error('Network time synchronization failed:', error.message);
    }
}

// Get adjusted time that's synchronized with network
function getAdjustedTime() {
    // If we haven't synced in a while or never synced, use local time
    if (Date.now() - lastTimeSyncTime > 300000) { // 5 minutes
        return Date.now();
    }
    
    return Date.now() + networkTimeOffset;
}

// Sync time immediately
syncNetworkTime();

// Sync time periodically
setInterval(syncNetworkTime, TIME_SYNC_INTERVAL);

// Utility function to get locked balances
async function getLockedBalances(publicKey) {
    if (!publicKey) return [];
    
    try {
        // Try multiple endpoints for redundancy
        const promises = HORIZON_URLS.map(url => 
            axios.get(`${url}/claimable_balances?claimant=${publicKey}&limit=100`, { timeout: 5000 })
                .then(response => response.data.records || [])
                .catch(() => []) // Silently fail individual requests
        );
        
        // Get results from any successful endpoint
        const results = await Promise.all(promises);
        const records = results.flat().filter(Boolean);
        
        if (!records.length) {
            return [];
        }
        
        return records.map(balance => {
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
            };
        });
    } catch (error) {
        console.error('Error fetching locked balances:', error);
        return [];
    }
}

// Socket.io connection handler
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    
    // Variables for this client
    let passphrase = null;
    let publicKey = null;
    let keypair = null;
    let bot = null;
    
    // Intercept console logs to forward to client
    const originalLog = console.log;
    const originalError = console.error;
    
    const forwardLog = (message, type = 'info') => {
        // Forward to client
        socket.emit('log', { 
            message: typeof message === 'object' ? JSON.stringify(message) : message.toString(),
            type 
        });
        
        // Also log to server console
        if (type === 'error') {
            originalError(message);
        } else {
            originalLog(message);
        }
    };
    
    // Override console methods for this connection
    console.log = forwardLog;
    console.error = (message) => forwardLog(message, 'error');
    
    // Handle login
    socket.on('login', async (data) => {
        try {
            // Validate mnemonic
            const mnemonic = data.passphrase.trim();
            
            try {
                // Test if we can derive a valid keypair
                keypair = StellarSdk.Keypair.fromSecret(
                    StellarSdk.getSeedFromMnemonic(mnemonic)
                );
                publicKey = keypair.publicKey();
                
                // Store passphrase securely for this connection
                passphrase = mnemonic;
                activePassphrases.set(socket.id, passphrase);
                
                // Get account info and balance
                const server = new StellarSdk.Server(HORIZON_URL);
                const account = await server.loadAccount(publicKey);
                
                const nativeBalance = account.balances.find(b => b.asset_type === 'native');
                const balance = nativeBalance ? nativeBalance.balance : '0';
                
                // Get locked balances
                const lockedBalances = await getLockedBalances(publicKey);
                
                socket.emit('login-response', {
                    success: true,
                    publicKey,
                    balance,
                    lockedBalances
                });
                
                console.log(`Logged in: ${publicKey}`);
            } catch (error) {
                throw new Error(`Invalid passphrase: ${error.message}`);
            }
        } catch (error) {
            console.error(`Login error: ${error.message}`);
            socket.emit('login-response', {
                success: false,
                error: error.message
            });
        }
    });
    
    // Handle balance refresh
    socket.on('refresh-balance', async () => {
        if (!publicKey) {
            socket.emit('log', { message: 'Not logged in', type: 'error' });
            return;
        }
        
        try {
            // Get account info and balance with redundancy
            let account = null;
            let error = null;
            
            for (const url of HORIZON_URLS) {
                try {
                    const server = new StellarSdk.Server(url);
                    account = await server.loadAccount(publicKey);
                    if (account) break;
                } catch (e) {
                    error = e;
                    continue;
                }
            }
            
            if (!account && error) {
                throw error;
            }
            
            const nativeBalance = account.balances.find(b => b.asset_type === 'native');
            const balance = nativeBalance ? nativeBalance.balance : '0';
            
            // Get locked balances
            const lockedBalances = await getLockedBalances(publicKey);
            
            socket.emit('balance-update', {
                balance,
                lockedBalances
            });
            
            console.log(`Balance refreshed for ${publicKey}: ${balance} Pi`);
        } catch (error) {
            console.error(`Balance refresh error: ${error.message}`);
            socket.emit('log', { message: `Error refreshing balance: ${error.message}`, type: 'error' });
        }
    });
    
    // Handle transaction history request
    socket.on('get-txn-history', async () => {
        if (!publicKey) {
            socket.emit('log', { message: 'Not logged in', type: 'error' });
            socket.emit('txn-history', { transactions: [] });
            return;
        }
        
        try {
            // Try multiple servers for redundancy
            let txns = null;
            let error = null;
            
            for (const url of HORIZON_URLS) {
                try {
                    const server = new StellarSdk.Server(url);
                    txns = await server.transactions()
                        .forAccount(publicKey)
                        .order('desc')
                        .limit(20)
                        .call();
                        
                    if (txns) break;
                } catch (e) {
                    error = e;
                    continue;
                }
            }
            
            if (!txns && error) {
                throw error;
            }
            
            const server = new StellarSdk.Server(HORIZON_URL);
            const transactions = await Promise.all(txns.records.map(async (txn) => {
                try {
                    // Get operations for this transaction
                    const ops = await server.operations()
                        .forTransaction(txn.hash)
                        .call();
                        
                    // Extract relevant details from operations
                    const operations = ops.records.map(op => ({
                        type: op.type,
                        amount: op.amount,
                        asset: op.asset_type === 'native' ? 'Pi' : op.asset_code,
                        from: op.from || op.source_account,
                        to: op.to || null
                    }));
                    
                    return {
                        hash: txn.hash,
                        date: new Date(txn.created_at).toISOString(),
                        fee: txn.fee_charged,
                        operations
                    };
                } catch (e) {
                    // Return partial information if operations fetch fails
                    return {
                        hash: txn.hash,
                        date: new Date(txn.created_at).toISOString(),
                        fee: txn.fee_charged,
                        operations: []
                    };
                }
            }));
            
            socket.emit('txn-history', { transactions });
        } catch (error) {
            console.error('Error fetching transaction history:', error);
            socket.emit('log', { message: `Error fetching transaction history: ${error.message}`, type: 'error' });
            socket.emit('txn-history', { transactions: [] });
        }
    });
    
    // Handle start bot with enhanced timing precision
    socket.on('start-bot', async (data) => {
        if (!passphrase) {
            socket.emit('log', { message: 'Not logged in', type: 'error' });
            return;
        }
        
        try {
            // Stop any existing bot
            if (bot) {
                bot.stop();
                bot = null;
            }
            
            // Create new bot instance with enhanced timing features
            bot = new PiNetworkTransferFloodBot({
                horizonUrls: HORIZON_URLS,
                networkPassphrase: NETWORK_PASSPHRASE,
                sourcePassphrase: passphrase,
                targetAddress: data.targetAddress,
                transferAmount: data.transferAmount,
                baseFee: data.baseFee || 3200000,
                unlockTime: data.unlockTime,
                preFloodSeconds: 5, // Increased from default for better preparation
                timeSyncIntervalMs: 30000, // Sync time every 30 seconds
                precisionTimerThreshold: 10 // Use high-precision timing when within 10 seconds
            });
            
            // Set logger to forward logs to client
            bot.setLogger((logData) => {
                socket.emit('log', logData);
            });
            
            // Store bot in active bots
            activeBots.set(socket.id, bot);
            
            // Start bot
            await bot.start();
            
            socket.emit('log', { message: 'Bot started successfully', type: 'success' });
            
        } catch (error) {
            console.error('Error starting bot:', error);
            socket.emit('log', { message: `Error starting bot: ${error.message}`, type: 'error' });
            
            // Clean up failed bot
            if (bot) {
                try {
                    bot.stop();
                } catch (e) {
                    // Ignore cleanup errors
                }
                bot = null;
                activeBots.delete(socket.id);
            }
        }
    });
    
    // Handle stop bot
    socket.on('stop-bot', () => {
        if (bot) {
            bot.stop();
            bot = null;
            activeBots.delete(socket.id);
            socket.emit('log', { message: 'Bot stopped', type: 'info' });
        } else {
            socket.emit('log', { message: 'No active bot to stop', type: 'warning' });
        }
    });
    
    // Handle network time sync request
    socket.on('sync-network-time', async () => {
        try {
            await syncNetworkTime();
            socket.emit('log', { 
                message: `Network time synchronized. Offset: ${networkTimeOffset}ms`,
                type: 'info'
            });
        } catch (error) {
            socket.emit('log', {
                message: `Failed to sync network time: ${error.message}`,
                type: 'error'
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
        
        // Remove stored passphrase
        activePassphrases.delete(socket.id);
        
        // Restore original console methods
        console.log = originalLog;
        console.error = originalError;
    });
});

// Start server
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Enhanced Pi Network Transfer Flood Bot Server started at ${new Date().toISOString()}`);
    console.log('Initializing network time synchronization...');
    
    // Initial time sync
    syncNetworkTime().then(() => {
        console.log('Ready to handle connections');
    }).catch(err => {
        console.error('Initial time sync failed:', err.message);
        console.log('Continuing with local time until sync succeeds');
    });
});