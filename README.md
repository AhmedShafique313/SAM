CAMMI/
│
├── .github/
│   └── workflows/
│       └── deploy.yml               # GitHub Actions workflow for CI/CD
│
└── CAMMI/
    ├── template.yaml                # Root SAM template (main orchestrator)
    ├── samconfig.toml               # SAM build/deploy configuration
    │
    ├── Layers/
    │   ├── template.yaml            # SAM layer definition
    │   └── layer_google.zip         # Zipped Python dependencies for Google login
    │
    ├── auth/
    │   ├── template.yaml            # SAM template for Auth (Google login Lambda)
    │   └── src/
    │       └── continue-with-google.py
    │
    └── feedback/
        ├── template.yaml            # SAM template for Feedback Lambda
        └── src/
            └── submit-feedback.py


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
