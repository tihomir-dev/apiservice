package customer.apiservice.util;

import java.util.*;
import org.springframework.http.ResponseEntity;

public class FieldValidator {

  public static ResponseEntity<Map<String, Object>> checkEmail(String email) {
    //REVIEW - in this instance add a null check and .isBlank check prior to the .matches -  If it's a null value, it will throw when trying to 
    //match nothing

    //REVIEW: You can create a Pattern Constant 
    //private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"); and use it wherever you
    //want to validate an email value
    if (!email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")) {
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error", "Invalid email format",
                  "field", "email",
                  "value", email));
    }

    return null; // Valid
  }

  public static ResponseEntity<Map<String, Object>> checkStatus(String status) {
    //REVIEW: possible nullPointerException - check if it's null or blank first before you try the equals
    if (!status.equalsIgnoreCase("ACTIVE") && !status.equalsIgnoreCase("INACTIVE")) {
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error", "Status must be ACTIVE or INACTIVE",
                  "field", "status",
                  "value", status));
    }

    return null; // Valid
  }

  /** Check if required fields are present and not empty */
  public static ResponseEntity<Map<String, Object>> checkRequired(
      Map<String, Object> data, String... fieldNames) {

    for (String fieldName : fieldNames) {
      Object value = data.get(fieldName);

      if (value == null) {
        return ResponseEntity.badRequest()
            .body(Map.of("error", fieldName + " is required", "field", fieldName));
      }

      //REVIEW: You don't need a nested if here, just append with && the check for empty strings to the first if
      if (value instanceof String) {
        String str = (String) value;
        if (str.trim().isEmpty()) {
          return ResponseEntity.badRequest()
              .body(Map.of("error", fieldName + " cannot be empty", "field", fieldName));
        }
      }
    }

    return null; // All valid
  }

  public static ResponseEntity<Map<String, Object>> checkAllowedValue(
      String fieldValue, String fieldName, String... allowedValues) {

    for (String allowed : allowedValues) {
      if (fieldValue.equalsIgnoreCase(allowed)) {
        return null;
      }
    }

    return ResponseEntity.badRequest()
        .body(
            Map.of(
                "error", fieldName + " must be one of: " + String.join(", ", allowedValues),
                "field", fieldName,
                "value", fieldValue));
  }

  /** Check if string length is valid */
  public static ResponseEntity<Map<String, Object>> checkLength(
      String value, String fieldName, int minLength, int maxLength) {

    if (value == null || value.isEmpty()) {
      return null;
    }

    int len = value.trim().length();

    if (len < minLength) {
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error",
                  fieldName + " must be at least " + minLength + " characters",
                  "field",
                  fieldName));
    }

    if (len > maxLength) {
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error",
                  fieldName + " must not exceed " + maxLength + " characters",
                  "field",
                  fieldName));
    }

    return null; // Valid
  }
}
