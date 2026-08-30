export interface DirtyTracker {
  buttonId: string;
  getValue: () => unknown;
  baseline: string;
}

export function normalizeSectionState(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeSectionState);
  }

  if (value && typeof value === "object") {
    return Object.keys(value as Record<string, unknown>)
      .sort()
      .reduce<Record<string, unknown>>((acc, key) => {
        const normalized = normalizeSectionState((value as Record<string, unknown>)[key]);
        if (normalized !== undefined) {
          acc[key] = normalized;
        }
        return acc;
      }, {});
  }

  return value;
}

export function serializeSectionState(value: unknown): string {
  return JSON.stringify(normalizeSectionState(value));
}

export function cloneSectionState<T>(value: T): T {
  return JSON.parse(serializeSectionState(value)) as T;
}

export function createDirtyTracker(
  sectionName: string,
  buttonId: string,
  getValue: () => unknown,
  previous: DirtyTracker | undefined,
  savedSections: Record<string, unknown>,
): [string, DirtyTracker] {
  const currentSerialized = serializeSectionState(getValue());
  const savedSnapshot = Object.prototype.hasOwnProperty.call(savedSections, sectionName)
    ? serializeSectionState(savedSections[sectionName])
    : currentSerialized;

  return [
    sectionName,
    {
      buttonId,
      getValue,
      baseline: previous?.baseline ?? savedSnapshot,
    },
  ];
}

export function isDirty(tracker: DirtyTracker): boolean {
  return serializeSectionState(tracker.getValue()) !== tracker.baseline;
}
