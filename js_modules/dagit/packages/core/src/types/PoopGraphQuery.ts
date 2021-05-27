// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: PoopGraphQuery
// ====================================================

export interface PoopGraphQuery_repositoriesOrError_PythonError {
  __typename: "PythonError";
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_declaredAssetKeys {
  __typename: "AssetKey";
  path: string[];
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_historicalAssetKeys {
  __typename: "AssetKey";
  path: string[];
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets_key {
  __typename: "AssetKey";
  path: string[];
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets {
  __typename: "Asset";
  id: string;
  key: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets_key;
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors {
  __typename: "Sensor";
  id: string;
  name: string;
  isAssetSensor: boolean | null;
  assets: (PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets | null)[] | null;
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
  declaredAssetKeys: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_declaredAssetKeys[];
  historicalAssetKeys: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_historicalAssetKeys[];
  sensors: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors[];
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes {
  __typename: "Repository";
  id: string;
  name: string;
  location: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_location;
  pipelines: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines[];
}

export interface PoopGraphQuery_repositoriesOrError_RepositoryConnection {
  __typename: "RepositoryConnection";
  nodes: PoopGraphQuery_repositoriesOrError_RepositoryConnection_nodes[];
}

export type PoopGraphQuery_repositoriesOrError = PoopGraphQuery_repositoriesOrError_PythonError | PoopGraphQuery_repositoriesOrError_RepositoryConnection;

export interface PoopGraphQuery {
  repositoriesOrError: PoopGraphQuery_repositoriesOrError;
}
