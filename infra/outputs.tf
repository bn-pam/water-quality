output "resource_group_name" {
  value = azurerm_resource_group.rg_data_project.name
}

output "adls_storage_account_name" {
  value = azurerm_storage_account.adls_storage.name
}

output "adls_container_names" {
  value = keys(azurerm_storage_data_lake_gen2_filesystem.adls_containers)
}