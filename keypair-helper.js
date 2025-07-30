/**
 * Pi Network Transfer Flood Bot - Keypair Helper
 * Date: 2025-07-30
 * 
 * Utility function to derive Stellar keypair from BIP-39 mnemonic passphrase
 */

const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const StellarSdk = require('stellar-sdk');

/**
 * Derive a Stellar keypair from a BIP-39 mnemonic passphrase
 * 
 * @param {string} passphrase - BIP-39 mnemonic passphrase (typically 12 or 24 words)
 * @param {string} derivationPath - HD wallet derivation path (default: Pi Network path)
 * @returns {StellarSdk.Keypair} Stellar keypair derived from the mnemonic
 */
function keypairFromPassphrase(passphrase, derivationPath = "m/44'/314159'/0'") {
    if (!passphrase) {
        throw new Error('Passphrase is required');
    }

    // Validate the mnemonic
    if (!bip39.validateMnemonic(passphrase)) {
        throw new Error('Invalid BIP39 mnemonic passphrase');
    }

    // Convert mnemonic to seed
    const seed = bip39.mnemonicToSeedSync(passphrase);

    // Derive the ED25519 key using the path
    const derived = derivePath(derivationPath, seed.toString('hex'));

    // Create Stellar keypair from the derived private key
    return StellarSdk.Keypair.fromRawEd25519Seed(Buffer.from(derived.key));
}

module.exports = { keypairFromPassphrase };