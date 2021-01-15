#!/bin/bash

nawk 'FNR==1 && NR!=1{next;}{print}' $@ | sed '/^\s*$/d'