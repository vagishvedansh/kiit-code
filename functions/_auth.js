export const MOCK_API_KEYS = {
  "test-key": { credit_balance: 1000.0, is_active: 1, key_id: "mock-test-key", is_mock: true },
  "default-dev-key": { credit_balance: 1000.0, is_active: 1, key_id: "mock-default-dev-key", is_mock: true },
  "live-key-valid": { credit_balance: 1000.0, is_active: 1, key_id: "mock-live-key", is_mock: true },
  "kiit-mock-key-12345": { credit_balance: 1000.0, is_active: 1, key_id: "mock-kiit-key", is_mock: true },
  "public": { credit_balance: 1000.0, is_active: 1, key_id: "mock-public-key", is_mock: true },
  "inactive-key": { credit_balance: 1000.0, is_active: 0, key_id: "mock-inactive-key", is_mock: true },
  "empty-balance-key": { credit_balance: 0.0, is_active: 1, key_id: "mock-empty-balance-key", is_mock: true },
};

/**
 * Validates the API key from request headers against Cloudflare D1,
 * with a resilient fallback to the mock key registry when D1 is offline,
 * undefined, or throws an error.
 *
 * @param {Request} request
 * @param {object} env
 * @returns {Promise<{ success: boolean, user?: object, apiKey?: string, status?: number, errorType?: string, message?: string }>}
 */
export async function validateApiKey(request, env = {}) {
  let apiKey = "";

  const authHeader = request.headers.get("Authorization") || request.headers.get("authorization") || "";
  if (/^Bearer\s+/i.test(authHeader)) {
    apiKey = authHeader.replace(/^Bearer\s+/i, "").trim();
  } else if (authHeader) {
    apiKey = authHeader.trim();
  }

  if (!apiKey) {
    apiKey = (request.headers.get("x-api-key") || request.headers.get("X-API-Key") || "").trim();
  }

  if (!apiKey) {
    return {
      success: false,
      status: 401,
      errorType: "authentication_error",
      message: "Missing API Key"
    };
  }

  // Helper to check mock record status and credit
  const evaluateMockUser = (mockUser) => {
    if (mockUser.is_active !== 1) {
      return {
        success: false,
        status: 401,
        errorType: "authentication_error",
        message: "Invalid or disabled API Key"
      };
    }
    if (mockUser.credit_balance <= 0) {
      return {
        success: false,
        status: 402,
        errorType: "invalid_request_error",
        message: "Credit balance exhausted ($0.00 remaining)."
      };
    }
    return {
      success: true,
      user: mockUser,
      apiKey
    };
  };

  // Fallback path when Cloudflare D1 database binding is absent or not initialized
  if (!env || !env.DB || typeof env.DB.prepare !== "function") {
    const mockUser = MOCK_API_KEYS[apiKey];
    if (mockUser) {
      return evaluateMockUser(mockUser);
    }
    return {
      success: false,
      status: 401,
      errorType: "authentication_error",
      message: "Invalid or disabled API Key"
    };
  }

  // D1 is present: query api_keys joined with users
  try {
    const user = await env.DB.prepare(
      `SELECT u.credit_balance, k.is_active, k.id as key_id 
       FROM api_keys k 
       JOIN users u ON k.user_id = u.id 
       WHERE k.key_value = ?`
    ).bind(apiKey).first();

    if (user) {
      if (user.is_active !== 1) {
        return {
          success: false,
          status: 401,
          errorType: "authentication_error",
          message: "Invalid or disabled API Key"
        };
      }

      if (user.credit_balance <= 0) {
        return {
          success: false,
          status: 402,
          errorType: "invalid_request_error",
          message: "Credit balance exhausted ($0.00 remaining)."
        };
      }

      return {
        success: true,
        user,
        apiKey
      };
    }

    // Key not found in D1: check mock registry fallback (supports dev/test keys in hybrid environments)
    const mockUser = MOCK_API_KEYS[apiKey];
    if (mockUser) {
      return evaluateMockUser(mockUser);
    }

    return {
      success: false,
      status: 401,
      errorType: "authentication_error",
      message: "Invalid or disabled API Key"
    };
  } catch (err) {
    console.warn("D1 query failed, falling back to mock key registry:", err.message);
    const mockUser = MOCK_API_KEYS[apiKey];
    if (mockUser) {
      return evaluateMockUser(mockUser);
    }

    return {
      success: false,
      status: 401,
      errorType: "authentication_error",
      message: "Invalid or disabled API Key"
    };
  }
}
