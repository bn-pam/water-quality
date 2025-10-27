terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.1"
    }
  }
  required_version = ">= 1.0"
}

# Configurer le fournisseur Azure
provider "azurerm" {
  features {}
}

# 1. Créer un Resource Group
resource "azurerm_resource_group" "rg_data_project" {
  name     = "rg-${var.project_name}"
  location = var.location

  tags = {
    Environment = "Dev"
    Project     = "Pipeline Qualité Eau"
    ManagedBy   = "Terraform"
  }
}

# Nécessaire pour un nom de compte de stockage globalement unique
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
  numeric = true
}

# 2. Créer un Azure Data Lake Storage Gen2
resource "azurerm_storage_account" "adls_storage" {
  # Nom globalement unique, ex: adlsqualiteeaufrancea1b2c3
  name                     = "adls${replace(var.project_name, "-", "")}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.rg_data_project.name
  location                 = azurerm_resource_group.rg_data_project.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # Local-redundant storage (moins cher pour dev)

  # Active le "Hierarchical Namespace" pour ADLS Gen2
  is_hns_enabled           = true

  tags = azurerm_resource_group.rg_data_project.tags

  # Dépend du groupe de ressources
  depends_on = [
    azurerm_resource_group.rg_data_project
  ]
}

# 3. Créer les conteneurs source (filesystems)
resource "azurerm_storage_data_lake_gen2_filesystem" "adls_containers" {
  # Crée un conteneur pour chaque élément dans la variable "containers"
  for_each = toset(var.containers)

  name               = each.key
  storage_account_id = azurerm_storage_account.adls_storage.id

  # Dépend du compte de stockage
  depends_on = [
    azurerm_storage_account.adls_storage
  ]
}

# 4. Créer un Azure Databricks Workspace
resource "azurerm_databricks_workspace" "databricks_ws" {
  # Nom globalement unique, ex: dbw-qualite-eau-france-a1b2c3
  name                = "dbw-${var.project_name}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.rg_data_project.name
  location            = azurerm_resource_group.rg_data_project.location
  sku = "standard"

  tags = azurerm_resource_group.rg_data_project.tags
  depends_on = [
    azurerm_resource_group.rg_data_project
  ]
}