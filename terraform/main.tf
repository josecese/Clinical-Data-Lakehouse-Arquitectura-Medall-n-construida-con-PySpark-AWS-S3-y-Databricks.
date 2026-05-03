# terraform/main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configura la región de AWS donde crearemos todo (puedes cambiarla si usas otra)
provider "aws" {
  region = "us-east-1" 
}