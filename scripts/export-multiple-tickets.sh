#!/bin/bash

# Set PYENV_ROOT and add to PATH
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

# Initialize pyenv (use --path for non-interactive shells)
eval "$(pyenv init --path)"
eval "$(pyenv virtualenv-init -)"

# Check if a filename is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <filename>"
  exit 1
fi

filename="$1"
command_to_run=(python -m src.exporter.cli export-ticket --format jsonl --ticket-id) # Replace 'echo' with your desired command

# Read the file line by line
while IFS= read -r line; do
  # Execute the command with the line content as a parameter
  "${command_to_run[@]}" "$line"
done < "$filename"
