import uuid
import boto3

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def create_bucket_name(bucket_prefix):
    # random generate bucket name using uuid
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response


def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


# copy objects between buckets
def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


# delete objects (Key and VersionID need to be Capitalised)
def delete_all_objects(bucket_name):
    res = []
    bucket = s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})


def main():
    '''
    # Create Bucket
    first_bucket_name, first_response = create_bucket(
        bucket_prefix='firstboto',
        s3_connection=s3_resource.meta.client)

    first_file_name = create_temp_file(300, 'firstfile.txt', 'f')

    print(first_file_name)
    first_bucket = s3_resource.Bucket(name=first_bucket_name)
    print(first_bucket)
    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=first_file_name
    )

    print(first_object)

    # uploading file to an object or to a bucket
    first_object.upload_file(first_file_name)

    # print(s3_resource.Object(bucket_name=first_bucket_name,key=first_bucket_name).download_file(f'C:\\ge.yang\Desktop'))

    # Buckets and Objects Traversal
    for bucket in s3_resource.buckets.all():
        print(bucket.name)

    first_bucket = s3_resource.Bucket(name='larry-tusk-practice')

    for obj in first_bucket.objects.all():
        print(obj.key)
    '''

    # delete bucket
    first_bucket_name = 'tute-geek'
    # s3_resource.Bucket(first_bucket_name).delete()
    # s3_resource.meta.client.delete_bucket(Bucket=first_bucket_name)
    delete_all_objects(first_bucket_name)

    '''
    copy_to_bucket('firstboto99725a2b-44e0-482b-a1bb-92a64ae06040', 'tute-geek', '883971firstfile.txt')

    # ACL control adjustment
    second_file_name = create_temp_file(400, 'secondfile.txt', 's')
    second_object = s3_resource.Object('tute-geek', second_file_name)
    second_object.upload_file(second_file_name, ExtraArgs={'ACL':'public-read'})

    second_object_acl = second_object.Acl()
    print(second_object_acl.grants)
    response = second_object_acl.put(ACL='private')
    print(second_object_acl.grants)

    # encryption and storage class adjustment
    third_file_name = create_temp_file(300, 'thirdfile.txt','t')
    third_object = s3_resource.Object('tute-geek', third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={
        'ServerSideEncryption': 'AES256',
        'StorageClass': 'STANDARD_IA'})

    print(third_object.server_side_encryption)
    print(third_object.storage_class)

    # bucket and object versioning
    first_file_name = create_temp_file(300, 'firstfile.txt', 'f')
    second_file_name = create_temp_file(400, 'secondfile.txt', 's')
    third_file_name = create_temp_file(400, 'thirdfile.txt', 's')

    s3_resource.Object('tute-geek', first_file_name).upload_file(first_file_name)
    s3_resource.Object('tute-geek', first_file_name).upload_file(second_file_name)
    s3_resource.Object('tute-geek', third_file_name).upload_file(third_file_name)

    def enable_bucket_versioning(bucket_name):
        bkt_versioning = s3_resource.BucketVersioning(bucket_name)
        bkt_versioning.enable()
        print(bkt_versioning.status)

    print(enable_bucket_versioning('tute-geek'))

    print(s3_resource.Object('tute-geek', first_file_name).version_id)
    '''


if __name__ == '__main__':
    main()



