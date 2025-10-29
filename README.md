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
    ├── Layers/
    │   ├── template.yaml            # Google libraries layer definition
    │   └── layer_google.zip         # Packaged dependencies
    │
    ├── dynamodb/
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



# google auth
aws secretsmanager create-secret \
  --name cammi-google-client-id \
  --secret-string "<YOUR_GOOGLE_CLIENT_ID>"

aws secretsmanager create-secret \
  --name cammi-google-client-secret \
  --secret-string "<YOUR_GOOGLE_CLIENT_SECRET>"

aws secretsmanager create-secret \
  --name cammi-zoho-app-password \
  --secret-string "<YOUR_ZOHO_APP_PASSWORD>"
