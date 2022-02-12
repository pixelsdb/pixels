#!/bin/bash

echo "Stop vmtouch."

kill $(pidof vmtouch)
