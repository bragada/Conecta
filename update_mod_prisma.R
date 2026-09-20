# Atualiza a planilha `historico_mod_pontos_luminosos` (id_ponto_servico -> potencia_total).
# Le do S3, nao da API -- ver o comentario em atualiza_prisma() abaixo.
# Pacotes vem da imagem Docker (ghcr.io/bragada/conecta) -> nao instalar em runtime.

library(googlesheets4)
library(gargle)
library(tidyverse)
library(aws.s3)
library(arrow)

gs4_auth(path = "sa.json")


########################################################################################
# Fonte: o parquet que o campinas.R ja grava no S3 -- nao a API.
#
# Por que mudou: a URL que este script usava (alias `webservice-consultarpontos
# modernizacaocompleto.json`) IGNORA o CMD_MODERNIZACAO e trava em filtro=2 ->
# devolve 0 linhas. Medido em 20/09/2026: HTTP 200, RESULT=1, zero registros.
# Como o erro nao aparece no status code, o script caia no guard de <=10 linhas e
# saia sem escrever nada: a planilha ficou parada desde 06/07/2026, em silencio.
#
# Ler o tt_mod_lum.parquet resolve e ainda evita pagar um segundo pull de ~18 min
# na API -- e exatamente a mesma base, gravada pelo campinas.R na mesma run.
atualiza_prisma <- function(objeto = "tt_mod_lum.parquet",
                            bucket = "automacao-conecta",
                            gsheet_url = "https://docs.google.com/spreadsheets/d/14wp-xTzqIonTzw6Y1sIq1BCffqOEfe45GIawx15ak5Q/edit") {

  dados <- tryCatch(
    aws.s3::s3read_using(FUN = arrow::read_parquet, object = objeto, bucket = bucket),
    error = function(e) { message("Falha ao ler ", objeto, " do S3: ", conditionMessage(e)); NULL })

  if (is.null(dados) || nrow(dados) <= 10) {
    message("Base vazia ou indisponivel -> planilha NAO sera atualizada.")
    return(invisible(NULL))
  }
  if (!all(c("potencia_lampada_atual", "id_ponto_servico") %in% names(dados))) {
    message("Colunas esperadas ausentes em ", objeto, " -> planilha NAO sera atualizada.")
    return(invisible(NULL))
  }

  mod_lum <- dados %>%
    select(potencia_lampada_atual, id_ponto_servico) %>%
    group_by(id_ponto_servico) %>%
    mutate(
        potencia_total = potencia_lampada_atual %>%
            # Substitui vírgula por ponto (caso existam decimais no padrão PT-BR)
            str_replace_all(",", ".") %>%
            # Divide a string pelo separador ";"
            str_split(";") %>%
            # Converte para numérico e soma (map_dbl garante que o resultado seja um número)
            map_dbl(~ sum(as.numeric(.x), na.rm = TRUE))
    ) %>%
    ungroup() %>%
    distinct(id_ponto_servico, potencia_total)

  message("prisma: ", nrow(mod_lum), " pontos -> ", gsheet_url)
  sheet_write(mod_lum, gsheet_url, sheet = "id_ponto_servico")
}

atualiza_prisma()
print('  Mod Lum (prisma) - Ok')
