package csx55.overlay.wireformats;

// Simple class to hold registration results
public class RegistrationResult {
    private final byte statusCode;
    private final String additionalInfo;

    public RegistrationResult(byte statusCode, String additionalInfo) {
        this.statusCode = statusCode;
        this.additionalInfo = additionalInfo;
    }

    public byte getStatusCode() {
        return statusCode;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }
}