pip install awscli -q
pip freeze
aws s3 cp --recursive ./ s3://s3-crypto-deployment-code/$APPVEYOR_REPO_BRANCH  --exclude "venv/*" --exclude ".git/*"
