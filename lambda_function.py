import json
import os
import requests
import boto3
import time
import shlex
from datetime import datetime
from io import BytesIO
import subprocess
from contextlib import closing

def tic():
    #require to import time
    global start_time_tictoc
    start_time_tictoc = time.time()


def toc(tag="elapsed time"):
    if "start_time_tictoc" in globals():
        print("{}: {:.9f} [sec]".format(tag, time.time() - start_time_tictoc))
    else:
        print("tic has not been called")

#-- LINE_CHANNEL_ACCESS_TOKENわすれずに！！！！
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
BUCKET_NAME='language-translate'

#--Headerの生成
HEADER = {
    'Content-type':
    'application/json',
    'Authorization':'Bearer ' + LINE_CHANNEL_ACCESS_TOKEN,
}

#-- 各種boto3群インスタンス
s3 = boto3.resource('s3') 
transcribe = boto3.client('transcribe')
translate = boto3.client('translate')
session = boto3.Session(region_name="ap-northeast-1")
polly = session.client("polly")
s3_client = boto3.client('s3')

'''関数群'''
def get_content_from_line(event):
    if event['message']['type']=='audio':
        MessageId = event['message']['id']  # メッセージID
        AudioFile = requests.get('https://api-data.line.me/v2/bot/message/'+ MessageId +'/content',headers=HEADER) #Audiocontent取得        
        Audio_bin = BytesIO(AudioFile.content)
        Audio = Audio_bin.getvalue()  # 音声取得
    else:
        Audio=None
    return Audio

def put_content_to_s3(content, bucket_name, bucket_key):
    obj = s3.Object(bucket_name,bucket_key)
    obj.put( Body=content )
    return 0
    
def speech_to_text(job_name, bucket_name, bucket_key_src, bucket_key_dst):
    job_uri = "s3://%s/%s"%(bucket_name,bucket_key_src)
    transcribe.start_transcription_job(
     TranscriptionJobName=job_name,
     Media={'MediaFileUri': job_uri},
     MediaFormat='mp4',
     LanguageCode='ja-JP',
     OutputBucketName=bucket_name,
     OutputKey=bucket_key_dst,
    )
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(5)
    return 0
    
def get_content_from_s3(bucket_name,bucket_key):
    obj = s3.Object(bucket_name,bucket_key)
    body = obj.get()['Body'].read() 
    json_data = json.loads(body.decode('utf-8'))
    print(json.dumps(json_data))
    transcript=json_data['results']['transcripts'][0]['transcript']
    return transcript    
    
def translate_transcript(transcript):
    res_trans = translate.translate_text(
        Text=transcript,
        SourceLanguageCode='ja',
        TargetLanguageCode='en',
    )
    res_text=res_trans['TranslatedText']
    print('original:%s'%transcript)
    print('translated:%s'%res_text)
    return res_text    

def synthesize_speech(text,bucket_name,output_key):
    bucket=s3.Bucket(bucket_name)
    response = polly.synthesize_speech(
        Text=text,
        Engine="neural",
        VoiceId="Joanna",
        OutputFormat="mp3",
        )
    with closing(response["AudioStream"]) as stream:
        bucket.put_object(Key=output_key, Body=stream.read())
    return 0

def get_signed_url(bucket_name, bucket_key):
    s3_source_signed_url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': bucket_name, 'Key': bucket_key},
        ExpiresIn=10,
        )
    return s3_source_signed_url    

def convert_mp3_to_aac(url, bucket_name, bucket_key_dst):
    ffmpeg_cmd = "/opt/bin/ffmpeg -i \"" + url + "\" -f adts -ab 32k -"
    #ffmpeg_cmd = "/opt/bin/ffmpeg -i \"" + url + "\" -f adts -c:a libfdk_aac -ab 32k -"
    command1 = shlex.split(ffmpeg_cmd)
    p1 = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    resp = s3_client.put_object(Body=p1.stdout, Bucket=bucket_name, Key=bucket_key_dst)
    return 0



'''
main function
'''
def lambda_handler(event, context):
    print('json dumps start --------')
    print(json.dumps(event)) #dict->str(json形式にエンコード)
    print('json dumps end --------')    # TODO implement

    body = json.loads(event['body'])
    for event in body['events']:
        
        #-- 1. get meta info
        userId=event['source']['userId']
        time_rcv=datetime.fromtimestamp(event['timestamp']/1000).strftime('%Y%m%dT%H%M%Sp%f')
        
        #-- 2. get content from line
        Audio=get_content_from_line(event)

        #-- 3. put content to s3
        key='m4aFromLine/%s.mp4'%time_rcv
        put_content_to_s3(Audio,BUCKET_NAME,key)


        #-- 4. transcribe
        speech_to_text(job_name=time_rcv,
                       bucket_name=BUCKET_NAME,
                       bucket_key_src=key,
                       bucket_key_dst='output/%s.json'%time_rcv
                      )

        #-- 5. get content from s3
        transcript=get_content_from_s3(BUCKET_NAME, bucket_key = 'output/%s.json'%time_rcv)

        #-- 6. translate
        translated_script=translate_transcript(transcript)

        #-- 7. polly
        synthesize_speech(translated_script, bucket_name=BUCKET_NAME, output_key='test1.mp3')

        #-- 8. get signed url from s3
        s3_source_signed_url=get_signed_url(bucket_name=BUCKET_NAME, bucket_key='test1.mp3')
        
        #-- 9. convert mp3 to aac
        convert_mp3_to_aac(s3_source_signed_url, bucket_name=BUCKET_NAME, bucket_key_dst='test1.aac')

        #-- 10. get url of output(aac)
        url=get_signed_url(BUCKET_NAME, 'test1.aac')
        
        #-- 11. set message
        REQUEST_MESSAGE = [
        {
        'type': 'audio',
        'originalContentUrl': url,
        'duration': 5000
        },
        {
        'type': 'text',
        'text': '%s\n%s'%(transcript,translated_script),
        }
        ]
        payload = {'to': userId, 'messages': REQUEST_MESSAGE}


        #-- 12. send message
        if len(payload['messages']) > 0:
                response = requests.post(
                'https://api.line.me/v2/bot/message/push',
                headers=HEADER,
                data=json.dumps(payload)
                )
                print('request sent!')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
