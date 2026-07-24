# Imagem base "rocker/verse": já vem com R + tidyverse + pandoc + LaTeX (tinytex),
# tudo instalado como binário (Posit Package Manager) -> build rápido e reproduzível.
# É o que resolve o relatório em PDF (pdf_document + kableExtra) sem instalar texlive na mão.
FROM rocker/verse:4.4.1

# Pacotes de sistema extras:
# - git / ca-certificates: necessários para o actions/checkout rodar dentro do container
# - libsodium-dev: usado por libs de credenciais/sodium
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# Pacotes R usados pelos scripts agendados.
# tidyverse / rmarkdown / knitr / tinytex JÁ vêm na imagem base (rocker/verse).
# install2.r baixa binários do Posit PM (rápido) e compila em paralelo (-n -1 = todos os núcleos).
RUN install2.r --error --skipinstalled -n -1 \
        janitor \
        arrow \
        aws.s3 \
        httr \
        jsonlite \
        base64enc \
        googlesheets4 \
        gargle \
        kableExtra \
        zoo

# Pré-instala os pacotes LaTeX usados pelo relatório em PDF (kableExtra exige 'tabu', etc.).
# Sem "|| true": se algum não instalar, queremos ver a falha aqui no build, não no render.
RUN R -e "tinytex::tlmgr_install(c('fancyhdr','geometry','booktabs','multirow','makecell','xcolor','colortbl','environ','trimspaces','etoolbox','wrapfig','float','ulem','threeparttable','threeparttablex','pdflscape','varwidth','tabu'))"
