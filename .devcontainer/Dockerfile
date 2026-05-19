FROM mcr.microsoft.com/devcontainers/go:2-1.25-trixie

ARG USERNAME=vscode

RUN mkdir -p \
  /workspace \
  /home/$USERNAME/.claude \
  /home/$USERNAME/.codex \
  && \
  chown -R $USERNAME:$USERNAME \
  /workspace \
  /home/$USERNAME/.claude \
  /home/$USERNAME/.codex

USER $USERNAME