# Script simplificado - apenas gera o PDF
cat("=== Iniciando geração do relatório ===\n")

# Instalar pacotes necessários
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

# Renderizar o relatório
cat("Renderizando relatório PDF...\n")
tryCatch({
  render_relatorio <- rmarkdown::render(
    input = "script_relatorio.Rmd",
    output_file = "program_conecta_campinas.pdf",
    quiet = FALSE
  )
  cat("✓ PDF gerado com sucesso!\n")
}, error = function(e) {
  cat(sprintf("✗ ERRO ao gerar PDF: %s\n", e$message))
  quit(status = 1)
})

# Verificar se o arquivo foi criado
if (!file.exists("program_conecta_campinas.pdf")) {
  cat("✗ ERRO: Arquivo PDF não foi encontrado!\n")
  quit(status = 1)
}

# Informações do arquivo
info <- file.info("program_conecta_campinas.pdf")
cat(sprintf("✓ Arquivo: program_conecta_campinas.pdf\n"))
cat(sprintf("✓ Tamanho: %.2f KB\n", info$size / 1024))
cat(sprintf("✓ Criado em: %s\n", info$mtime))

cat("\n=== PDF pronto para envio! ===\n")
