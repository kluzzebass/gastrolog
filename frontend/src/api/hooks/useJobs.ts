import { useQuery } from "@tanstack/react-query";
import { jobClient } from "../client";
import { JobStatus } from "../gen/gastrolog/v1/job_pb";

export function useJob(jobId: string | null) {
  return useQuery({
    queryKey: ["job", jobId],
    queryFn: async () => {
      const response = await jobClient.getJob({ id: jobId! });
      return response.job;
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === JobStatus.COMPLETED || status === JobStatus.FAILED)
        return false;
      return 1000;
    },
  });
}

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: async () => {
      const response = await jobClient.listJobs({});
      return response.jobs;
    },
    refetchInterval: 5000,
  });
}
