
set_minio_shared()


aws.s3::copy_object(
  from_object = "MobiCount/fv/csg_02_19/fv_csg_indoor_02_2019.csv", 
  to_object = "MobiCount/fv/csg_02_19/indoor_02_19.csv", 
  from_bucket = "etgtk6", 
  to_bucket = "etgtk6"
)


put_local_file("minior-master.tar.gz", 
               dir_minio = "MobiCount/fv/csg_02_19", bucket = "etgtk6")
