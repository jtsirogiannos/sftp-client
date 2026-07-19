package io.github.jtsirogiannos.sftp.client.api;

import java.time.Instant;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Optional;

/**
 * A partial change to a remote file's attributes — the write side of {@link RemoteFile}.
 *
 * <p>Partial on purpose, because the protocol is: {@code SSH_FXP_SETSTAT} carries an attributes
 * structure with a flags word saying which fields are actually present, and the server leaves the
 * rest alone. Modelling that as "set everything" would mean a caller who wants to change the mode
 * first reading the file's owner and timestamps back so it could write them again unchanged — and
 * racing anyone else who touched the file in between.
 *
 * <pre>{@code
 * client.setAttributes("/outbound/report.csv", AttributeUpdate.builder()
 *         .permissions(0600)
 *         .build());
 * }</pre>
 *
 * <p>An empty update is legal and does nothing.
 */
public record AttributeUpdate(
        OptionalInt permissions,
        OptionalLong size,
        Optional<Instant> modifiedTime,
        OptionalInt ownerId,
        OptionalInt groupId) {

    public static Builder builder() {
        return new Builder();
    }

    /** True when nothing at all is set, so a provider can skip the round trip. */
    public boolean isEmpty() {
        return permissions.isEmpty() && size.isEmpty() && modifiedTime.isEmpty()
                && ownerId.isEmpty() && groupId.isEmpty();
    }

    public static final class Builder {

        private OptionalInt permissions = OptionalInt.empty();
        private OptionalLong size = OptionalLong.empty();
        private Optional<Instant> modifiedTime = Optional.empty();
        private OptionalInt ownerId = OptionalInt.empty();
        private OptionalInt groupId = OptionalInt.empty();

        private Builder() {
        }

        /** POSIX mode bits, e.g. {@code 0600}. Write it in octal or the value will surprise you. */
        public Builder permissions(int mode) {
            this.permissions = OptionalInt.of(mode);
            return this;
        }

        /**
         * Truncates or extends the file. Setting a size below the current one discards the tail —
         * this is the protocol's only truncate.
         */
        public Builder size(long bytes) {
            this.size = OptionalLong.of(bytes);
            return this;
        }

        /**
         * Last-modified time. SFTP v3 carries whole seconds, so sub-second precision is lost on most
         * servers.
         */
        public Builder modifiedTime(Instant when) {
            this.modifiedTime = Optional.of(when);
            return this;
        }

        /**
         * Numeric uid. Names are not used: SFTP v3 reports and accepts numbers, and resolving them
         * needs a shell round trip this library does not make — the same reason {@link RemoteFile}
         * has no owner name.
         */
        public Builder ownerId(int uid) {
            this.ownerId = OptionalInt.of(uid);
            return this;
        }

        /** Numeric gid; see {@link #ownerId}. */
        public Builder groupId(int gid) {
            this.groupId = OptionalInt.of(gid);
            return this;
        }

        public AttributeUpdate build() {
            return new AttributeUpdate(permissions, size, modifiedTime, ownerId, groupId);
        }
    }
}
