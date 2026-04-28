package io.kestra.plugin.compress;

public enum KeyDerivation {
    PBKDF2_SHA256,
    PBKDF2_SHA512,
    ARGON2ID,
    SCRYPT
}
