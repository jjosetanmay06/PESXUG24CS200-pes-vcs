// object.c — Content-addressable object store
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions: object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the filesystem path where an object should be stored.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Write an object to the store.
// Format on disk: "<type> <size>\0<data>"
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Build the full object: header + \0 + data
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    // Full object = header + '\0' + data
    size_t obj_len = (size_t)header_len + 1 + len;

    uint8_t *obj = malloc(obj_len);
    if (!obj) return -1;

    memcpy(obj, header, (size_t)header_len);
    obj[header_len] = '\0';
    memcpy(obj + header_len + 1, data, len);

    // Step 2: Compute SHA-256 of the full object
    ObjectID id;
    compute_hash(obj, obj_len, &id);

    // Step 3: Deduplication — if object already exists, skip writing
    if (object_exists(&id)) {
        free(obj);
        *id_out = id;
        return 0;
    }

    // Step 4: Create shard directory .pes/objects/XX/
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // Ignore error if already exists

    // Get final path
    char path[512];
    object_path(&id, path, sizeof(path));

    // Step 5: Write to a temporary file in the shard directory
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(obj); return -1; }

    ssize_t written = write(fd, obj, obj_len);
    free(obj);
    if (written != (ssize_t)obj_len) { close(fd); unlink(tmp_path); return -1; }

    // Step 6: fsync the temp file to ensure data reaches disk
    fsync(fd);
    close(fd);

    // Step 7: Atomically rename temp file to final path
    if (rename(tmp_path, path) != 0) { unlink(tmp_path); return -1; }

    // Step 8: fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }

    // Step 9: Store the computed hash in *id_out
    *id_out = id;
    return 0;
}

// Read an object from the store and verify its integrity.
// Returns 0 on success, -1 on error.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Build the file path
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read the entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size_l = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (file_size_l <= 0) { fclose(f); return -1; }
    size_t file_size = (size_t)file_size_l;

    uint8_t *buf = malloc(file_size);
    if (!buf) { fclose(f); return -1; }

    if (fread(buf, 1, file_size, f) != file_size) {
        free(buf); fclose(f); return -1;
    }
    fclose(f);

    // Step 3: Parse header — find the '\0' separating header from data
    uint8_t *null_byte = memchr(buf, '\0', file_size);
    if (!null_byte) { free(buf); return -1; }

    // Step 4: Verify integrity by recomputing SHA-256
    ObjectID computed;
    compute_hash(buf, file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf); return -1; // Corruption detected
    }

    // Step 5: Parse the type string
    if      (strncmp((char *)buf, "blob ",   5) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)buf, "tree ",   5) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)buf, "commit ", 7) == 0) *type_out = OBJ_COMMIT;
    else { free(buf); return -1; }

    // Step 6: Extract the data portion (after the '\0')
    size_t data_start = (size_t)(null_byte - buf) + 1;
    size_t data_len   = file_size - data_start;

    void *data = malloc(data_len + 1); // +1 for safety null terminator
    if (!data) { free(buf); return -1; }
    memcpy(data, buf + data_start, data_len);
    ((char *)data)[data_len] = '\0';

    free(buf);
    *data_out = data;
    *len_out  = data_len;
    return 0;
}
