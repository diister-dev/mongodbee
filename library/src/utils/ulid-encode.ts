const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ULID_PATTERN = /^[0-9A-HJKMNP-TV-Z]{26}$/;

export function encodeUlidTime(ms: number): string {
  let rest = ms;
  const chars = new Array<string>(10);
  for (let i = 9; i >= 0; i--) {
    chars[i] = CROCKFORD[rest % 32];
    rest = Math.floor(rest / 32);
  }
  return chars.join("");
}

export function encodeUlidRandom(bytes: Uint8Array): string {
  let n = 0n;
  for (let i = 0; i < 10; i++) n = (n << 8n) | BigInt(bytes[i] ?? 0);
  const chars = new Array<string>(16);
  for (let i = 15; i >= 0; i--) {
    chars[i] = CROCKFORD[Number(n & 31n)];
    n >>= 5n;
  }
  return chars.join("");
}

export function isUlid(uid: string): boolean {
  return ULID_PATTERN.test(uid.toUpperCase());
}
