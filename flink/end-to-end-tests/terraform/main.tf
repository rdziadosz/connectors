module "networking" {
  source = "./modules/networking"

  availability_zone1 = var.availability_zone1
  availability_zone2 = var.availability_zone2
}

module "storage" {
  source = "./modules/storage"

  test_data_bucket_name = var.test_data_bucket_name
}

module "cloudwatch" {
  source = "./modules/cloudwatch"

  cloudwatch_group_name = var.cloudwatch_group_name
}

module "flink_session_cluster" {
  source = "./modules/flink-session-cluster"

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  cloudwatch_group_id = module.cloudwatch.cloudwatch_group_id

  region                = var.region
  availability_zone1    = var.availability_zone1
  availability_zone2    = var.availability_zone2
  test_data_bucket_name = var.test_data_bucket_name

  depends_on = [module.networking, module.storage, module.cloudwatch]
}
