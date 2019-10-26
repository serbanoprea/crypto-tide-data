pip install awscli -q
pip freeze
awscli configure set aws_access_key_id $AWS_ACCESS_KEY_ID
awscli configure set secret_access_key $AWS_SECRET_ACCESS_KEY
awscli configure set default.region eu-west-1
aws s3 cp --recursive ./ s3://s3-crypto-deployment-code/$APPVEYOR_REPO_BRANCH  --exclude "venv/*" --exclude ".git/*" --dryrun
