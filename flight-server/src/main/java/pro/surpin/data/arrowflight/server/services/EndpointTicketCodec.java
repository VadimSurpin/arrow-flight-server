package pro.surpin.data.arrowflight.server.services;

import pro.surpin.data.arrowflight.server.model.HandleState;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;

/**
 * Encodes endpoint state into authenticated self-contained Flight ticket handles.
 */
final class EndpointTicketCodec {

    private static final int MAGIC = 0x41465431;
    private static final int SIGNATURE_BYTES = 32;
    private static final int MAX_FIELD_BYTES = 16 * 1024 * 1024;
    private static final int MAX_FILE_COUNT = 1_000_000;
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final byte[] secret;

    /**
     * Creates a codec using a cluster-shared signing secret.
     *
     * @param secret cluster ticket secret
     */
    EndpointTicketCodec(byte[] secret) {
        if (secret == null || secret.length < SIGNATURE_BYTES) {
            throw new IllegalArgumentException("Ticket secret must contain at least 32 bytes");
        }
        this.secret = secret.clone();
    }

    /**
     * Encodes and signs endpoint state.
     *
     * @param state endpoint state
     * @return authenticated ticket handle bytes
     */
    byte[] encode(HandleState state) {
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(payloadBytes)) {
                output.writeInt(MAGIC);
                writeString(output, state.query());
                String[] paths = state.filePaths();
                output.writeInt(paths == null ? -1 : paths.length);
                if (paths != null) {
                    for (String path : paths) {
                        writeString(output, path);
                    }
                }
                writeString(output, state.serverUri());
                output.writeLong(state.bytes());
                output.writeBoolean(state.loadTracked());
            }
            byte[] payload = payloadBytes.toByteArray();
            byte[] signature = sign(payload);
            byte[] result = new byte[payload.length + signature.length];
            System.arraycopy(payload, 0, result, 0, payload.length);
            System.arraycopy(signature, 0, result, payload.length, signature.length);
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to encode Flight endpoint ticket", e);
        }
    }

    /**
     * Verifies and decodes endpoint state.
     *
     * @param encoded authenticated ticket handle bytes
     * @return decoded endpoint state
     */
    HandleState decode(byte[] encoded) {
        if (!isEncoded(encoded) || encoded.length <= SIGNATURE_BYTES) {
            throw new IllegalArgumentException("Unsupported Flight endpoint ticket");
        }
        int payloadLength = encoded.length - SIGNATURE_BYTES;
        byte[] payload = new byte[payloadLength];
        byte[] signature = new byte[SIGNATURE_BYTES];
        System.arraycopy(encoded, 0, payload, 0, payloadLength);
        System.arraycopy(encoded, payloadLength, signature, 0, SIGNATURE_BYTES);
        if (!MessageDigest.isEqual(signature, sign(payload))) {
            throw new IllegalArgumentException("Invalid Flight endpoint ticket signature");
        }

        try (DataInputStream input =
                     new DataInputStream(new ByteArrayInputStream(payload))) {
            if (input.readInt() != MAGIC) {
                throw new IllegalArgumentException("Unsupported Flight endpoint ticket");
            }
            String query = readString(input);
            int pathCount = input.readInt();
            if (pathCount < -1 || pathCount > MAX_FILE_COUNT) {
                throw new IllegalArgumentException("Invalid Flight endpoint file count");
            }
            String[] paths = pathCount < 0 ? null : new String[pathCount];
            if (paths != null) {
                for (int i = 0; i < paths.length; i++) {
                    paths[i] = readString(input);
                }
            }
            String serverUri = readString(input);
            long bytes = input.readLong();
            boolean loadTracked = input.readBoolean();
            if (input.available() != 0) {
                throw new IllegalArgumentException("Unexpected Flight endpoint ticket data");
            }
            return new HandleState(query, paths, serverUri, bytes, loadTracked);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed Flight endpoint ticket", e);
        }
    }

    /**
     * Checks whether bytes use the self-contained endpoint ticket format.
     *
     * @param encoded candidate ticket handle
     * @return true when the format marker is present
     */
    boolean isEncoded(byte[] encoded) {
        return encoded != null
                && encoded.length >= Integer.BYTES + SIGNATURE_BYTES
                && readInt(encoded) == MAGIC;
    }

    /**
     * Writes a nullable UTF-8 string with an integer byte length.
     *
     * @param output destination stream
     * @param value nullable string
     * @throws IOException on stream failure
     */
    private static void writeString(DataOutputStream output, String value)
            throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_FIELD_BYTES) {
            throw new IllegalArgumentException("Flight endpoint ticket field is too large");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    /**
     * Reads a nullable bounded UTF-8 string.
     *
     * @param input source stream
     * @return decoded nullable string
     * @throws IOException on stream failure
     */
    private static String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > MAX_FIELD_BYTES || length > input.available()) {
            throw new IllegalArgumentException("Invalid Flight endpoint ticket field length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    /**
     * Signs ticket payload bytes.
     *
     * @param payload serialized ticket payload
     * @return HMAC signature
     */
    private byte[] sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
            return mac.doFinal(payload);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to sign Flight endpoint ticket", e);
        }
    }

    /**
     * Reads a big-endian integer without allocating a stream.
     *
     * @param bytes source bytes
     * @return decoded integer
     */
    private static int readInt(byte[] bytes) {
        return (bytes[0] & 0xff) << 24
                | (bytes[1] & 0xff) << 16
                | (bytes[2] & 0xff) << 8
                | bytes[3] & 0xff;
    }
}
