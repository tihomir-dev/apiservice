package customer.apiservice.util;

import org.springframework.http.ResponseEntity;
import java.util.*;

/**
 * Simple field validation utility.
 * Replaces repetitive validation code in your controllers.
 */
public class FieldValidator {

    /**
     * Check if email is valid
     * Returns error response or null if valid
     */
    public static ResponseEntity<Map<String, Object>> checkEmail(String email) {
        if (email == null || email.isEmpty()) {
            return null; // Optional field - OK if empty
        }
        
        if (!email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Invalid email format",
                "field", "email",
                "value", email
            ));
        }
        
        return null; // Valid
    }

    /**
     * Check if status is valid (ACTIVE or INACTIVE)
     * Returns error response or null if valid
     */
    public static ResponseEntity<Map<String, Object>> checkStatus(String status) {
        if (status == null || status.isEmpty()) {
            return null; // Optional field - OK if empty
        }
        
        if (!status.equalsIgnoreCase("ACTIVE") && !status.equalsIgnoreCase("INACTIVE")) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Status must be ACTIVE or INACTIVE",
                "field", "status",
                "value", status
            ));
        }
        
        return null; // Valid
    }

    /**
     * Check if required fields are present and not empty
     * Returns error response or null if all valid
     */
    public static ResponseEntity<Map<String, Object>> checkRequired(
            Map<String, Object> data,
            String... fieldNames) {
        
        for (String fieldName : fieldNames) {
            Object value = data.get(fieldName);
            
            // Check if null
            if (value == null) {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", fieldName + " is required",
                    "field", fieldName
                ));
            }
            
            // Check if empty string
            if (value instanceof String) {
                String str = (String) value;
                if (str.trim().isEmpty()) {
                    return ResponseEntity.badRequest().body(Map.of(
                        "error", fieldName + " cannot be empty",
                        "field", fieldName
                    ));
                }
            }
        }
        
        return null; // All valid
    }

    /**
     * Check if field is one of allowed values
     * Example: checkAllowedValue(userType, "employee", "contractor", "admin")
     */
    public static ResponseEntity<Map<String, Object>> checkAllowedValue(
            String fieldValue,
            String fieldName,
            String... allowedValues) {
        
        if (fieldValue == null || fieldValue.isEmpty()) {
            return null; // Optional field - OK if empty
        }
        
        for (String allowed : allowedValues) {
            if (fieldValue.equalsIgnoreCase(allowed)) {
                return null; // Valid - matches one of allowed values
            }
        }
        
        return ResponseEntity.badRequest().body(Map.of(
            "error", fieldName + " must be one of: " + String.join(", ", allowedValues),
            "field", fieldName,
            "value", fieldValue
        ));
    }

    /**
     * Check if string length is valid
     */
    public static ResponseEntity<Map<String, Object>> checkLength(
            String value,
            String fieldName,
            int minLength,
            int maxLength) {
        
        if (value == null || value.isEmpty()) {
            return null; // Optional field - OK if empty
        }
        
        int len = value.trim().length();
        
        if (len < minLength) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", fieldName + " must be at least " + minLength + " characters",
                "field", fieldName
            ));
        }
        
        if (len > maxLength) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", fieldName + " must not exceed " + maxLength + " characters",
                "field", fieldName
            ));
        }
        
        return null; // Valid
    }
}