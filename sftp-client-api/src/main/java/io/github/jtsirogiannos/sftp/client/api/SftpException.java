package io.github.jtsirogiannos.sftp.client.api;

/**
 * The single failure type this library throws. Providers map their backend's exceptions onto this —
 * {@code com.jcraft.jsch.SftpException} and MINA's {@code SftpException} must never reach a caller,
 * because leaking them would make the choice of provider visible in user code and defeat the whole
 * "bring your own driver" premise.
 *
 * <p>Unchecked on purpose. A checked exception here would force {@code try/catch} into every lambda
 * in the reactive path, where failures are carried by the {@link java.util.concurrent.CompletionStage}
 * rather than thrown.
 *
 * <p>The {@link #status()} field carries the SFTP protocol status code when the failure came from a
 * server response (RFC 4253 {@code SSH_FX_*}), so callers can distinguish "no such file" from
 * "permission denied" without string-matching the message. It is {@link #NO_STATUS} for local
 * failures — connection refused, auth rejected, I/O error mid-transfer.
 */
public class SftpException extends RuntimeException {

    /**
     * Status for failures that did not originate in an SFTP server response.
     */
    public static final int NO_STATUS = -1;
    // Common SSH_FX_* codes, here so providers and callers agree on the mapping.
    public static final int SSH_FX_OK = 0;
    public static final int SSH_FX_EOF = 1;
    public static final int SSH_FX_NO_SUCH_FILE = 2;
    public static final int SSH_FX_PERMISSION_DENIED = 3;
    public static final int SSH_FX_FAILURE = 4;
    public static final int SSH_FX_FILE_ALREADY_EXISTS = 11;
    private static final long serialVersionUID = 1L;
    private final int status;

    public SftpException(String message) {
        this(message, null, NO_STATUS);
    }

    public SftpException(String message, Throwable cause) {
        this(message, cause, NO_STATUS);
    }

    public SftpException(String message, Throwable cause, int status) {
        super(message, cause);
        this.status = status;
    }

    /**
     * The {@code SSH_FX_*} status code, or {@link #NO_STATUS} if this was not a server response.
     */
    public int status() {
        return status;
    }

    /**
     * True when the server said the path does not exist.
     */
    public boolean isNoSuchFile() {
        return status == SSH_FX_NO_SUCH_FILE;
    }

    /**
     * True when the server refused the operation on permission grounds.
     */
    public boolean isPermissionDenied() {
        return status == SSH_FX_PERMISSION_DENIED;
    }

    /**
     * True when {@link io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode#CREATE_NEW} lost
     * the race against an existing path.
     */
    public boolean isAlreadyExists() {
        return status == SSH_FX_FILE_ALREADY_EXISTS;
    }
}
