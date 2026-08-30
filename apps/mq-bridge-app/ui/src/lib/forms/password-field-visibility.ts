export function isEnvironmentReference(value: string) {
  const trimmed = value.trim();
  return trimmed.startsWith("${") && trimmed.endsWith("}");
}

export function looksLikeCredentialUrl(value: string) {
  const trimmed = value.trim();
  const schemeSeparator = trimmed.indexOf("://");
  const authorityStart = schemeSeparator >= 0 ? schemeSeparator + 3 : 0;
  const authorityTail = trimmed.slice(authorityStart);
  const authorityEnd = authorityTail.search(/[/?#]/);
  const authority = authorityEnd >= 0 ? authorityTail.slice(0, authorityEnd) : authorityTail;
  const atIndex = authority.lastIndexOf("@");

  if (atIndex < 0) {
    return false;
  }

  const credentials = authority.slice(0, atIndex);
  return credentials.includes(":");
}

export function defaultPasswordFieldVisibility(fieldName: string, value: string) {
  if (isEnvironmentReference(value)) {
    return true;
  }

  if (/url/i.test(fieldName)) {
    return !looksLikeCredentialUrl(value);
  }

  return false;
}
