# Script para gerar apenas o PDF
cat("=== Iniciando geração do relatório ===\n")

# Instalar pacotes
packages <- c("rmarkdown", "tidyverse")
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("Instalando %s...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}

library(rmarkdown)
library(tidyverse)

# Verificar Pandoc
cat(sprintf("Versão do Pandoc: %s\n", pandoc_version()))

# Renderizar
cat("Renderizando relatório PDF...\n")
tryCatch({
  render_relatorio <- rmarkdown::render(
    input = "script_relatorio.Rmd",
    output_file = "program_conecta_campinas.pdf",
    quiet = FALSE
  )
  cat("✓ PDF gerado com sucesso!\n")
}, error = function(e) {
  cat(sprintf("✗ ERRO: %s\n", e$message))
  quit(status = 1)
})

# Verificar
if (!file.exists("program_conecta_campinas.pdf")) {
  cat("✗ Arquivo não encontrado!\n")
  quit(status = 1)
}

info <- file.info("program_conecta_campinas.pdf")
cat(sprintf("✓ Tamanho: %.2f KB\n", info$size / 1024))
cat("=== PDF pronto! ===\n")
