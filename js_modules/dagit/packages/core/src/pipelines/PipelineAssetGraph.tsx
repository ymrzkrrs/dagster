import {gql} from '@apollo/client';
import {Icon, Tooltip} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {RunStatus} from '../runs/RunStatusDots';
import {titleForRun} from '../runs/RunUtils';
import {Box} from '../ui/Box';
import {Group} from '../ui/Group';

import {PipelineAssetGraphPipelineFragment} from './types/PipelineAssetGraphPipelineFragment';

export const PipelineAssetGraph = ({pipeline}: {pipeline: PipelineAssetGraphPipelineFragment}) => {
  console.log(pipeline);
  return (
    <div>
      <PipelineNode pipeline={pipeline} />
    </div>
  );
};

const PipelineNode = ({pipeline}: {pipeline: PipelineAssetGraphPipelineFragment}) => (
  <Box>
    <Icon icon={IconNames.DIAGRAM_TREE} />
    {pipeline.name}
    <Group direction="row" spacing={4} alignItems="center">
      {pipeline.runs.slice(0, 3).map((run) => (
        <Link key={run.runId} to={`/instance/runs/${run.runId}`}>
          <Tooltip
            position={'top'}
            content={titleForRun(run)}
            wrapperTagName="div"
            targetTagName="div"
          >
            <RunStatus status={run.status} />
          </Tooltip>
        </Link>
      ))}

      {pipeline.runs.length ? (
        <Box margin={{left: 4}}>
          <Link to={`/instance/runs/${pipeline.runs[0].runId}`}>
            {titleForRun(pipeline.runs[0])}
          </Link>
        </Box>
      ) : null}
    </Group>
  </Box>
);

export const PIPELINE_ASSET_GRAPH_FRAGMENT = gql`
  fragment PipelineAssetGraphPipelineFragment on PipelineSnapshot {
    __typename
    id
    name
    description
    runs(limit: 5) {
      id
      runId
      status
    }
    schedules {
      id
      name
      scheduleState {
        id
        status
        runs(limit: 3) {
          id
          runId
          pipelineName
          status
        }
      }
    }
    sensors {
      id
      name
      sensorState {
        id
        status
        runs(limit: 3) {
          id
          runId
          pipelineName
          status
        }
      }
      isAssetSensor
      assets {
        id
        sourcePipelines {
          id
          name
        }
      }
    }
  }
`;
