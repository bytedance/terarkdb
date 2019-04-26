#!/bin/bash

pkgdir=pkg
mkdir -p $pkgdir
cp -r db        $pkgdir
cp -r env       $pkgdir
cp -r include   $pkgdir
cp -r memtable  $pkgdir
cp -r port      $pkgdir
cp -r table     $pkgdir
cp -r util      $pkgdir
