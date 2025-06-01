#!/usr/bin/env python3
"""
This script creates a Bedrock Knowledge Base with multiple data sources.
It checks for existing resources before creating new ones.
"""

import os
import sys
import time
import boto3
import logging
import pprint
import json
import warnings
from pathlib import Path
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

def check_s3_bucket_exists(s3_client, bucket_name):
    """Check if an S3 bucket exists"""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 bucket {bucket_name} already exists")
        return True
    except Exception:
        logger.info(f"S3 bucket {bucket_name} does not exist")
        return False

def create_s3_bucket_if_not_exists(s3_client, bucket_name, region):
    """Create an S3 bucket if it doesn't exist"""
    if check_s3_bucket_exists(s3_client, bucket_name):
        return True
    
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        logger.info(f"Created S3 bucket {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Error creating S3 bucket {bucket_name}: {e}")
        return False

def check_opensearch_collection_exists(aoss_client, collection_name):
    """Check if an OpenSearch Serverless collection exists"""
    try:
        collections = aoss_client.batch_get_collection(names=[collection_name])
        if collections.get('collectionDetails'):
            logger.info(f"OpenSearch collection {collection_name} already exists")
            return collections['collectionDetails'][0]
        return None
    except Exception as e:
        logger.error(f"Error checking OpenSearch collection: {e}")
        return None

def get_existing_knowledge_base(bedrock_agent_client, kb_name):
    """Get a handle to an existing knowledge base"""
    try:
        # List knowledge bases and find the one with the matching name
        kbs = bedrock_agent_client.list_knowledge_bases(maxResults=100)
        kb_id = None
        
        for kb in kbs.get('knowledgeBaseSummaries', []):
            if kb['name'] == kb_name:
                kb_id = kb['knowledgeBaseId']
                logger.info(f"Found existing knowledge base with ID: {kb_id}")
                break
                
        if kb_id:
            response = bedrock_agent_client.get_knowledge_base(knowledgeBaseId=kb_id)
            kb = response['knowledgeBase']
            
            # Get data sources for this KB
            ds_list = bedrock_agent_client.list_data_sources(
                knowledgeBaseId=kb_id,
                maxResults=100
            ).get('dataSourceSummaries', [])
            
            data_sources = []
            for ds in ds_list:
                ds_response = bedrock_agent_client.get_data_source(
                    dataSourceId=ds['dataSourceId'],
                    knowledgeBaseId=kb_id
                )
                data_sources.append(ds_response['dataSource'])
                
            return kb, data_sources
        else:
            logger.info(f"Knowledge base {kb_name} not found, will create a new one")
            return None, None
    except Exception as e:
        logger.error(f"Error getting existing knowledge base: {e}")
        return None, None

def upload_directory(s3_client, local_path, bucket_name):
    """Upload a directory to an S3 bucket"""
    logger.info(f"Uploading directory {local_path} to bucket {bucket_name}")
    for root, dirs, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = os.path.relpath(file_path, local_path)
            logger.info(f"Uploading file {file_path} to {bucket_name}/{s3_key}")
            try:
                s3_client.upload_file(file_path, bucket_name, s3_key)
            except Exception as e:
                logger.error(f"Error uploading file {file_path}: {e}")

def main():
    """Main function to create a knowledge base"""
    try:
        # Create AWS session using default credentials (from SSO)
        session = boto3.session.Session()
        region = session.region_name
        
        # Create AWS clients
        s3_client = session.client('s3')
        sts_client = session.client('sts')
        bedrock_agent_client = session.client('bedrock-agent')
        bedrock_agent_runtime_client = session.client('bedrock-agent-runtime')
        aoss_client = session.client('opensearchserverless')
        
        # Get account ID
        account_id = sts_client.get_caller_identity().get('Account')
        logger.info(f"Using AWS account: {account_id} in region: {region}")
        
        # Create a unique suffix for resource names
        timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))[-7:]
        suffix = f"{timestamp_str}"
        
        # Define resource names
        knowledge_base_name = f"bedrock-sample-knowledge-base-{suffix}"
        knowledge_base_description = "Multi data source knowledge base."
        data_bucket_name = f'bedrock-kb-{suffix}-1'
        vector_store_name = f'bedrock-sample-rag-{suffix}'
        
        # Define data sources
        data_sources = [
            {"type": "S3", "bucket_name": data_bucket_name}
            # Add other data sources as needed
        ]
        
        kbAlreadyExists = True
        if kbAlreadyExists:
            knowledge_base_name = "bedrock-sample-knowledge-base-0232519"
            knowledge_base_description = "Multi data source knowledge base."
            data_bucket_name = 'bedrock-kb-0232519-1'
            vector_store_name = 'bedrock-sample-rag-0232519-f'

            # Get  existing knowledge base
            existing_kb, existing_data_sources = get_existing_knowledge_base(bedrock_agent_client, knowledge_base_name)


            logger.info(f"Using existing knowledge base: {knowledge_base_name}")
            knowledge_base = existing_kb
            data_sources = existing_data_sources
            
            # Import the BedrockKnowledgeBase class
            sys.path.append(str(Path(__file__).resolve().parent))
            from utils.knowledge_base import BedrockKnowledgeBase
            
            # Create a wrapper around the existing KB
            kb_wrapper = BedrockKnowledgeBase(data_sources=data_sources, createKB=False, existingKB=knowledge_base)

            
            # Override the knowledge_base and data_source attributes
            kb_wrapper.knowledge_base = knowledge_base
            kb_wrapper.data_source = data_sources
            
            knowledge_base = kb_wrapper
        else:
            logger.info(f"Creating new knowledge base: {knowledge_base_name}")
            
            # Check if OpenSearch collection exists
            existing_collection = check_opensearch_collection_exists(aoss_client, vector_store_name)
            
            # Import the BedrockKnowledgeBase class
            sys.path.append(str(Path(__file__).resolve().parent))
            from utils.knowledge_base import BedrockKnowledgeBase
            
            # Create a new knowledge base
            knowledge_base = BedrockKnowledgeBase(
                kb_name=knowledge_base_name,
                kb_description=knowledge_base_description,
                data_sources=data_sources,
                chunking_strategy="FIXED_SIZE",
                suffix=suffix
            )
        
            # Upload data to S3 bucket
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "synthetic_dataset")
            if os.path.exists(data_dir):
                upload_directory(s3_client, data_dir, data_bucket_name)
            else:
                logger.warning(f"Data directory {data_dir} does not exist, skipping upload")
            
            # Start ingestion job
            logger.info("Starting ingestion job...")
            knowledge_base.start_ingestion_job()
        
        # Get knowledge base ID for testing
        kb_id = knowledge_base.knowledge_base['knowledgeBaseId']
        logger.info(f"Knowledge base ID: {kb_id}")
        
        # Test the knowledge base with a query
        query = "Provide a summary of consolidated statements of cash flows of Octank Financial for the fiscal years ended December 31, 2019?"
        foundation_model = "amazon.nova-micro-v1:0"
        

        logger.info(f"Testing knowledge base with query: {query}")
        # Loop requesting cli input until user enters 'exit'
        while True:
            query = input("\nEnter your query (or 'exit' to quit): ")
            if query.lower() == 'exit':
                break

#            start_time = time()
            try:
                response = bedrock_agent_runtime_client.retrieve_and_generate(
                    input={"text": query},
                    retrieveAndGenerateConfiguration={
                        "type": "KNOWLEDGE_BASE",
                        "knowledgeBaseConfiguration": {
                            'knowledgeBaseId': kb_id,
                            "modelArn": f"arn:aws:bedrock:{region}::foundation-model/{foundation_model}",
                            "retrievalConfiguration": {
                                "vectorSearchConfiguration": {
                                    "numberOfResults": 5
                                }
                            }
                        }
                    }
                )
                
                print("\nRetrieve and Generate Response:")
                print(response['output']['text'])
#                print(f"\nResponse (took {time() - start_time:.2f} seconds):")
            except Exception as e:
                print(f"Error processing query: {str(e)}")
                print("Try reformulating your question")

        
        # Test retrieval only
        logger.info("Testing retrieval only...")
        retrieval_query = "How many new positions were opened across Amazon's fulfillment and delivery network?"
        response_ret = bedrock_agent_runtime_client.retrieve(
            knowledgeBaseId=kb_id,
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": 5,
                }
            },
            retrievalQuery={
                "text": retrieval_query
            }
        )
        
        print("\nRetrieval Results:")
        for num, chunk in enumerate(response_ret.get('retrievalResults', []), 1):
            print(f'Chunk {num}: {chunk["content"]["text"]}')
            print(f'Chunk {num} Location: {chunk["location"]}')
            print(f'Chunk {num} Score: {chunk["score"]}')
            print(f'Chunk {num} Metadata: {chunk["metadata"]}')
            print()
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()