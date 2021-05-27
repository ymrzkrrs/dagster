// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus, JobStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: PipelineAssetGraphPipelineFragment
// ====================================================

export interface PipelineAssetGraphPipelineFragment_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface PipelineAssetGraphPipelineFragment_schedules_scheduleState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  pipelineName: string;
  status: PipelineRunStatus;
}

export interface PipelineAssetGraphPipelineFragment_schedules_scheduleState {
  __typename: "JobState";
  id: string;
  status: JobStatus;
  runs: PipelineAssetGraphPipelineFragment_schedules_scheduleState_runs[];
}

export interface PipelineAssetGraphPipelineFragment_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  scheduleState: PipelineAssetGraphPipelineFragment_schedules_scheduleState;
}

export interface PipelineAssetGraphPipelineFragment_sensors_sensorState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  pipelineName: string;
  status: PipelineRunStatus;
}

export interface PipelineAssetGraphPipelineFragment_sensors_sensorState {
  __typename: "JobState";
  id: string;
  status: JobStatus;
  runs: PipelineAssetGraphPipelineFragment_sensors_sensorState_runs[];
}

export interface PipelineAssetGraphPipelineFragment_sensors_assets_sourcePipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
}

export interface PipelineAssetGraphPipelineFragment_sensors_assets {
  __typename: "Asset";
  id: string;
  sourcePipelines: PipelineAssetGraphPipelineFragment_sensors_assets_sourcePipelines[];
}

export interface PipelineAssetGraphPipelineFragment_sensors {
  __typename: "Sensor";
  id: string;
  name: string;
  sensorState: PipelineAssetGraphPipelineFragment_sensors_sensorState;
  isAssetSensor: boolean | null;
  assets: (PipelineAssetGraphPipelineFragment_sensors_assets | null)[] | null;
}

export interface PipelineAssetGraphPipelineFragment {
  __typename: "PipelineSnapshot";
  id: string;
  name: string;
  description: string | null;
  runs: PipelineAssetGraphPipelineFragment_runs[];
  schedules: PipelineAssetGraphPipelineFragment_schedules[];
  sensors: PipelineAssetGraphPipelineFragment_sensors[];
}
