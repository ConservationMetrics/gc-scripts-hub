// THIS FILE IS READ-ONLY
// AND GENERATED AUTOMATICALLY FROM YOUR RUNNABLES

export declare const backend: {
  apply_import: (args: {
    import_id: string;
    preview_id: string;
  }) => Promise<any>;
  check_dataset_name: (args: { dataset_name: string }) => Promise<any>;
  list_datasets: (args: {}) => Promise<any>;
  preview_import: (args: {
    import_id: string;
    identity_fields?: any;
    update_policy?: string;
  }) => Promise<any>;
  stage_import: (args: {
    uploaded_file: any;
    goal: string;
    target_table: string;
    replace_import_id?: string;
  }) => Promise<any>;
};

export declare const backendAsync: {
  apply_import: (args: {
    import_id: string;
    preview_id: string;
  }) => Promise<string>;
  check_dataset_name: (args: { dataset_name: string }) => Promise<string>;
  list_datasets: (args: {}) => Promise<string>;
  preview_import: (args: {
    import_id: string;
    identity_fields?: any;
    update_policy?: string;
  }) => Promise<string>;
  stage_import: (args: {
    uploaded_file: any;
    goal: string;
    target_table: string;
    replace_import_id?: string;
  }) => Promise<string>;
};

export type Job = {
  type: "QueuedJob" | "CompletedJob";
  id: string;
  created_at: number;
  started_at: number | undefined;
  duration_ms: number;
  success: boolean;
  args: any;
  result: any;
};

/**
 * Execute a job and wait for it to complete and return the completed job
 * @param id
 */
export declare function waitJob(id: string): Promise<Job>;

/**
 * Get a job by id and return immediately with the current state of the job
 * @param id
 */
export declare function getJob(id: string): Promise<Job>;

export type StreamUpdate = {
  new_result_stream?: string;
  stream_offset?: number;
};

/**
 * Stream job results using SSE. Calls onUpdate for each stream update,
 * and resolves with the final result when the job completes.
 * @param id - The job ID to stream
 * @param onUpdate - Optional callback for stream updates with new_result_stream data
 * @returns Promise that resolves with the final job result
 */
export declare function streamJob(
  id: string,
  onUpdate?: (data: StreamUpdate) => void,
): Promise<any>;
