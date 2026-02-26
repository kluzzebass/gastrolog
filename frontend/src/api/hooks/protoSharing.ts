/**
 * Custom structuralSharing for TanStack Query with protobuf messages.
 *
 * Proto message instances are not plain objects, so react-query's default
 * replaceEqualDeep always returns the new reference — triggering re-renders
 * on every refetch even when data hasn't changed. These helpers use proto's
 * own `equals` to preserve referential identity when data is unchanged.
 */

type SharingFn = (oldData: unknown, newData: unknown) => unknown;
type ProtoEquals = (a: any, b: any) => boolean;

/** For queries that return a single proto message. */
export function protoSharing(equals: ProtoEquals): SharingFn {
  return (oldData, newData) => {
    if (oldData !== undefined && equals(oldData, newData)) return oldData;
    return newData;
  };
}

/** For queries that return an array of proto messages. */
export function protoArraySharing(equals: ProtoEquals): SharingFn {
  return (oldData, newData) => {
    const oldArr = oldData as unknown[] | undefined;
    const newArr = newData as unknown[];
    if (!oldArr?.length || oldArr.length !== newArr.length) return newData;
    for (let i = 0; i < oldArr.length; i++) {
      if (!equals(oldArr[i], newArr[i])) return newData;
    }
    return oldData;
  };
}
