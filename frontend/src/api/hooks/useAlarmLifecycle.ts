import { useMutation, useQueryClient } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import type { SystemAlert } from "../gen/gastrolog/v1/cluster_pb";

/** The wire type of SystemAlert.id — what every mutation addresses. */
type AlarmId = SystemAlert["id"];

/** Alarm lifecycle mutations (gastrolog-1z5gg4). Servable from any node:
 *  the backend resolves every raiser of the alarm ID and fans the
 *  operation out — partial failures surface as errors naming the
 *  unreachable node, and the operations are idempotent, so retrying is
 *  always safe. Success refetches cluster status so the panel reflects the
 *  new state without waiting for the next broadcast tick. */
export function useAlarmLifecycle() {
  const qc = useQueryClient();
  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["clusterStatus"] });
  };

  const ack = useMutation({
    mutationFn: async (alarmId: AlarmId) => {
      await lifecycleClient.ackAlarm({ alarmId });
    },
    onSuccess: refresh,
  });

  const shelve = useMutation({
    mutationFn: async (args: { alarmId: AlarmId; durationSeconds: number }) => {
      await lifecycleClient.shelveAlarm({
        alarmId: args.alarmId,
        durationSeconds: BigInt(args.durationSeconds),
      });
    },
    onSuccess: refresh,
  });

  const unshelve = useMutation({
    mutationFn: async (alarmId: AlarmId) => {
      await lifecycleClient.unshelveAlarm({ alarmId });
    },
    onSuccess: refresh,
  });

  return { ack, shelve, unshelve };
}
