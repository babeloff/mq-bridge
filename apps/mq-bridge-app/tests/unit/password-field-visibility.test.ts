import { describe, expect, test } from "vitest";
import {
  defaultPasswordFieldVisibility,
  isEnvironmentReference,
  looksLikeCredentialUrl,
} from "../../ui/src/lib/forms/password-field-visibility";

describe("password-field visibility", () => {
  test("keeps non-url secrets masked by default", () => {
    expect(defaultPasswordFieldVisibility("api_token", "super-secret")).toBe(false);
  });

  test("shows plain url values by default", () => {
    expect(defaultPasswordFieldVisibility("database_url", "postgres://localhost:5432/app")).toBe(true);
  });

  test("masks url values that include credentials", () => {
    expect(
      defaultPasswordFieldVisibility("database_url", "postgres://user:password@localhost:5432/app"),
    ).toBe(false);
    expect(looksLikeCredentialUrl("postgres://user:password@localhost:5432/app")).toBe(true);
  });

  test("shows environment variable placeholders by default", () => {
    expect(defaultPasswordFieldVisibility("database_url", "${DATABASE_URL}")).toBe(true);
    expect(isEnvironmentReference("${DATABASE_URL}")).toBe(true);
  });
});
