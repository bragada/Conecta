
if (!requireNamespace("emayili", quietly = TRUE)) install.packages("emayili")
if (!requireNamespace("keyring", quietly = TRUE)) install.packages("keyring")
if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("blastula", quietly = TRUE)) install.packages("blastula")

library(keyring)
library(tidyverse)
library(rmarkdown)
library(blastula)
library(emayili)
Sys.getenv("GMAIL_AUTH")

print(Sys.getenv("SMTP_USER"))
print(Sys.getenv("SMTP_PASS"))


rmarkdown::pandoc_version()
render_relatorio <- rmarkdown::render(
  input = "script_relatorio.Rmd",
  output_file = "program_conecta_campinas.pdf"
)


info_relatorio <- file.info("program_conecta_campinas.pdf")

if(as.Date(info_relatorio$mtime,tz = "America/Sao_Paulo") == Sys.Date()){

  

smtp <- server(
  host = "smtp.gmail.com",
  port = 587,
  username = Sys.getenv("SMTP_USER"),
  password = Sys.getenv("SMTP_PASS")
)

# Criar o email e anexar o arquivo temporário
email <- envelope() %>%
  from("rikibragada@gmail.com") %>%
  to("rikibragada@gmail.com") %>%
  subject("Programação Conecta - Prefeitura") %>%
  text("Bom dia,  

  Segue em anexo a programação diária das manutenções e modernizações previstas para a cidade de Campinas.  

  Bot - HK CONSULTORIA") %>%
  attachment(path = "program_conecta_campinas.pdf")

# Enviar o email
smtp(email, verbose = TRUE)


} else {
  
}
