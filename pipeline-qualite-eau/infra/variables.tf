variable "location" {
  description = "Région Azure où déployer les ressources."
  type        = string
  default     = "France Central"
}

variable "project_name" {
  description = "Nom du projet utilisé pour nommer les ressources."
  type        = string
  default     = "qualite-eau-france"
}

variable "containers" {
  description = "Liste des conteneurs (filesystems) à créer dans l'ADLS."
  type        = list(string)
  default     = ["raw", "bronze", "silver", "gold"]
}