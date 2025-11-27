SAM/
│
├── .github/
│   └── workflows/
│       └── deploy.yml               # CI/CD automation for SAM build & deploy
│
└── CAMMI/
    ├── template.yaml                # 🧩 Root SAM orchestrator (nested stacks)
    ├── samconfig.toml               # Build/deploy configuration
    │
    ├── layers/
    │   ├── template.yaml            # Google libraries layer definition
    │   └── layer_google.zip         # Packaged dependencies
    │
    ├── db/
    │   └── template.yaml            # ✅ Contains users & feedback DynamoDB tables
    │
    ├── auth/
    │   ├── template.yaml            # Google OAuth Lambda definition
    │   └── src/
    │       └── continue-with-google.py
    │
    ├── API/
    │   └── template.yaml            # API Gateway resources & methods
    │
    └── feedback/
        ├── template.yaml            # Customer feedback Lambdas (2 functions)
        └── src/
            ├── customer-feedback.py
            └── check-customer-feedback.py

ok 


### Definition of Unified State Machine with websockets:

{
  "Comment": "CAMMI State Machine Definition",
  "StartAt": "clientIDSelect",
  "States": {
    "clientIDSelect": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${ClientIDRegistrationLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "getNextTier"
    },

    "getNextTier": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${GetNextPendingTierLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "processMap"
    },

    "processMap": {
      "Type": "Map",
      "ItemProcessor": {
        "ProcessorConfig": { "Mode": "INLINE" },
        "StartAt": "bedrockCore",
        "States": {
          "bedrockCore": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "OutputPath": "$.Payload",
            "Parameters": {
              "FunctionName": "${StateMachineStarterLambda}",
              "Payload.$": "$"
            },
            "Retry": [
              {
                "ErrorEquals": [
                  "Lambda.ServiceException",
                  "Lambda.AWSLambdaException",
                  "Lambda.SdkClientException",
                  "Lambda.TooManyRequestsException"
                ],
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 2,
                "JitterStrategy": "FULL"
              }
            ],
            "End": true
          }
        }
      },
      "Next": "eventCreator"
    },

    "eventCreator": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${EventCreatorLambda}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "textToFrontEnd"
    },

    "textToFrontEnd": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${RealTimeTextFrontendLambda}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "creatorEvent"
    },

    "creatorEvent": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${CreatorEventLambda}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "tierStatusUpdate"
    },

    "tierStatusUpdate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${UpdateTierStatusLambda}",
        "Payload.$": "$"
        
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "progressBarStatus"
    },

    "progressBarStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${ProgressBarLambda}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "isFinalIteration"
    },

    "isFinalIteration": {
      "Type": "Choice",
      "Choices": [
        {
          "Next": "getNextTier",
          "Variable": "$.next_iteration",
          "BooleanEquals": false
        }
      ],
      "Default": "finalDocumentCreation"
    },

    "finalDocumentCreation": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${DocumentationLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "docCompleteEvent"
    },

    "docCompleteEvent": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${DocumentCompleteEventLambda}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "End": true
    }
  }
}


### Unified State Machine without websockets:
{
  "Comment": "CAMMI State Machine Definition",
  "StartAt": "clientIDSelect",
  "States": {
    "clientIDSelect": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${ClientIDRegistrationLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "getNextTier"
    },

    "getNextTier": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${GetNextPendingTierLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "processMap"
    },

    "processMap": {
      "Type": "Map",
      "ItemProcessor": {
        "ProcessorConfig": { "Mode": "INLINE" },
        "StartAt": "bedrockCore",
        "States": {
          "bedrockCore": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "OutputPath": "$.Payload",
            "Parameters": {
              "FunctionName": "${StateMachineStarterLambda}",
              "Payload.$": "$"
            },
            "Retry": [
              {
                "ErrorEquals": [
                  "Lambda.ServiceException",
                  "Lambda.AWSLambdaException",
                  "Lambda.SdkClientException",
                  "Lambda.TooManyRequestsException"
                ],
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 2,
                "JitterStrategy": "FULL"
              }
            ],
            "End": true
          }
        }
      },
      "Next": "tierStatusUpdate"
    },

    "tierStatusUpdate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${UpdateTierStatusLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "isFinalIteration"
    },

    "isFinalIteration": {
      "Type": "Choice",
      "Choices": [
        {
          "Next": "getNextTier",
          "Variable": "$.next_iteration",
          "BooleanEquals": false
        }
      ],
      "Default": "finalDocumentCreation"
    },

    "finalDocumentCreation": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${DocumentationLambda}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "End": true
    }
  }
}


# git:

      - name: Deploy Rest API Endpoints
        run: |
          REST_API_NAME="cammi-endpoints"
          REGION="us-east-1"
          REST_API_ID="3gd0sb22ah"
          echo "Deploying REST API: $REST_API_ID"
          aws apigateway create-deployment \
            --rest-api-id $REST_API_ID \
            --region $REGION \
            --stage-name dev


### OLD Websocket
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: API Template for CAMMI Websockets with X-Ray Tracing and Access Logging

Resources:
  EditHeadingWebSocketApi:
    Type: AWS::ApiGatewayV2::Api
    Properties:
      Name: EditHeading
      ProtocolType: WEBSOCKET
      RouteSelectionExpression: "$request.body.action"

  EditHeadingLambdaWebSocketIntegration:
    Type: AWS::ApiGatewayV2::Integration
    Properties:
      ApiId: !Ref EditHeadingWebSocketApi
      IntegrationType: AWS_PROXY
      IntegrationUri: !Sub
        - arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${EditHeadingFunctionArn}/invocations
        - EditHeadingFunctionArn: !ImportValue EditHeadingFunctionArn
      CredentialsArn: !ImportValue CAMMI-ApiGatewayFunctionRoleArn
      IntegrationMethod: POST

  ConnectRoute:
    Type: AWS::ApiGatewayV2::Route
    Properties:
      ApiId: !Ref CAMMIWebSocketApi
      RouteKey: "$connect"
      OperationName: ConnectRoute
      Target: !Sub "integrations/${UploadTextExtractFunctionWebSocketLambdaIntegration}"

  DisconnectRoute:
    Type: AWS::ApiGatewayV2::Route
    Properties:
      ApiId: !Ref CAMMIWebSocketApi
      RouteKey: "$disconnect"
      OperationName: DisconnectRoute
      Target: !Sub "integrations/${UploadTextExtractFunctionWebSocketLambdaIntegration}"

  DefaultRoute:
    Type: AWS::ApiGatewayV2::Route
    Properties:
      ApiId: !Ref CAMMIWebSocketApi
      RouteKey: "$default"
      OperationName: DefaultRoute
      Target: !Sub "integrations/${UploadTextExtractFunctionWebSocketLambdaIntegration}"

  EditHeadingRoute:
    Type: AWS::ApiGatewayV2::Route
    Properties:
      ApiId: !Ref CAMMIWebSocketApi
      RouteKey: "editHeading"
      Target: !Sub "integrations/${EditHeadingLambdaWebSocketIntegration}"

  RealtimeTextFrontendRoute:
    Type: AWS::ApiGatewayV2::Route
    Properties:
      ApiId: !Ref CAMMIWebSocketApi
      RouteKey: "realtimeText"
      Target: !Sub "integrations/${RealtimeTextFrontendLambdaWebSocketIntegration}"


  WebSocketLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub /aws/apigateway/${CAMMIWebSocketApi}-access-logs
      RetentionInDays: 14

  CAMMIWebSocketDeployment:
    Type: AWS::ApiGatewayV2::Deployment
    DependsOn:
      - ConnectRoute
      - DisconnectRoute
      - DefaultRoute
      - EditHeadingRoute
      - RealtimeTextFrontendRoute
    Properties:
      ApiId: !Ref CAMMIWebSocketApi

  CAMMIWebSocketStage:
    Type: AWS::ApiGatewayV2::Stage
    DependsOn: CAMMIWebSocketDeployment
    Properties:
      StageName: dev
      Description: dev Stage with tracing enabled
      ApiId: !Ref CAMMIWebSocketApi
      DeploymentId: !Ref CAMMIWebSocketDeployment
      AccessLogSettings:
        DestinationArn: !GetAtt WebSocketLogGroup.Arn
        Format: |
          {"requestId":"$context.requestId","requestTime":"$context.requestTime","routeKey":"$context.routeKey","status":"$context.status","connectionId":"$context.connectionId"}
      DefaultRouteSettings:
        DataTraceEnabled: true
        LoggingLevel: INFO
        DetailedMetricsEnabled: true

Outputs:
  CAMMIWebSocketApiId:
    Description: The ID of the CAMMI WebSocket API
    Value: !Ref CAMMIWebSocketApi
    Export:
      Name: CAMMI-WebSocketApiId-v1

  CAMMIWebSocketApiEndpoint:
    Description: The WebSocket API endpoint URL
    Value: !Sub wss://${CAMMIWebSocketApi}.execute-api.${AWS::Region}.amazonaws.com/dev
    Export:
      Name: CAMMI-WebSocketApiEndpoint-v1
