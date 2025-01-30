install.packages("blastula")
install.packages("rmarkdown")
install.packages("keyring")
library(keyring)

library(tidyverse)
library(rmarkdown)
library(blastula)
rmarkdown::pandoc_version()
render_relatorio <- rmarkdown::render(
  input = "script_relatorio.Rmd",
  output_file = "program_conecta_campinas.pdf"
)


info_relatorio <- file.info("program_conecta_campinas.pdf")

if(as.Date(info_relatorio$mtime,tz = "America/Sao_Paulo") == Sys.Date()){

  email <- compose_email(
    body = md("
  Bom dia,  

  Segue em anexo a programação diária das manutenções e modernizações previstas para a cidade de Campinas.  

  Bot - HK CONSULTORIA
  ")
  )
  
  email <- email %>%
    add_attachment(
      file = "program_conecta_campinas.pdf",  # Nome do arquivo que será anexado
      content_type = "application/pdf"  # Tipo de arquivo anexado
    )
  
  destinatarios <- c("rikibragada@gmail.com")
  # 4. Envio do email com as credenciais armazenadas
  email %>%
    smtp_send(
      from = "hkbragada@gmail.com",
      to = destinatarios,  
      subject = "Programação Conecta - Prefeitura",  
      credentials =  creds_file("email_auth.json")
    )

} else {
  
}
