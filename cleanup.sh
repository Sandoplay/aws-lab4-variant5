#!/usr/bin/env bash
# Прибирання ресурсів лаб.4 (варіант 5). Потрібен налаштований AWS CLI.
#   bash cleanup.sh
# Кожну команду можна виконати й вручну в консолі – див. розділ «Прибирання» в README.md.
set -u
REGION=eu-north-1
BUCKET=amogusadmin-lab4-ingest
TABLE=lab4-ingest
QUEUE=lab4-ingest-failures
FUNCTIONS=(lab4-ingest-json lab4-ingest-csv)
LAYER=lab4-common
ROLE=lab4-ingest-json-role-xfz1sy8e
POLICY_NAME=Lab4IngestPolicy
ALARMS=(lab4-dlq-not-empty lab4-lambda-errors)
export AWS_REGION=$REGION

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "account: $ACCOUNT_ID, region: $REGION"

echo "== S3: прибрати сповіщення, очистити й видалити бакет"
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --notification-configuration '{}' || true
aws s3 rm "s3://$BUCKET" --recursive || true
# якщо вмикали версіонування – видалити і всі версії/маркери
LEFT=$(aws s3api list-object-versions --bucket "$BUCKET" --query 'length(Versions || `[]`)' --output text 2>/dev/null || echo 0)
[ "${LEFT:-0}" != "0" ] && echo "увага: лишилось $LEFT версій об'єктів – видаліть їх у консолі (Show versions)"
aws s3api delete-bucket --bucket "$BUCKET" || true

echo "== DynamoDB"
aws dynamodb delete-table --table-name "$TABLE" || true

echo "== SQS"
QURL=$(aws sqs get-queue-url --queue-name "$QUEUE" --query QueueUrl --output text 2>/dev/null) && \
  aws sqs delete-queue --queue-url "$QURL" || true

echo "== CloudWatch alarms"
aws cloudwatch delete-alarms --alarm-names "${ALARMS[@]}" || true

echo "== Lambda: функції та всі версії шару"
for f in "${FUNCTIONS[@]}"; do aws lambda delete-function --function-name "$f" || true; done
for v in $(aws lambda list-layer-versions --layer-name "$LAYER" --query 'LayerVersions[].Version' --output text 2>/dev/null); do
  aws lambda delete-layer-version --layer-name "$LAYER" --version-number "$v" || true
done

echo "== IAM: від'єднати й видалити політики, видалити роль"
POLICY_ARN="arn:aws:iam::$ACCOUNT_ID:policy/$POLICY_NAME"
for arn in $(aws iam list-attached-role-policies --role-name "$ROLE" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
  aws iam detach-role-policy --role-name "$ROLE" --policy-arn "$arn"
done
for p in $(aws iam list-role-policies --role-name "$ROLE" --query 'PolicyNames[]' --output text 2>/dev/null); do
  aws iam delete-role-policy --role-name "$ROLE" --policy-name "$p"
done
aws iam delete-role --role-name "$ROLE" || true
aws iam delete-policy --policy-arn "$POLICY_ARN" || true
# керована політика, яку консоль генерує разом з роллю (AWSLambdaBasicExecutionRole-<uuid>)
for arn in $(aws iam list-policies --scope Local --query "Policies[?starts_with(PolicyName,'AWSLambdaBasicExecutionRole')].Arn" --output text); do
  echo "знайдено згенеровану політику $arn – видаліть, якщо вона від цієї лаби: aws iam delete-policy --policy-arn $arn"
done

echo "== CloudWatch Logs"
for f in "${FUNCTIONS[@]}"; do aws logs delete-log-group --log-group-name "/aws/lambda/$f" || true; done

echo "Готово. Наступного дня перевірте Cost Explorer і Billing -> Free tier."
