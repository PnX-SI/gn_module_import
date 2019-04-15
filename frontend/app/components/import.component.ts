import { Component, OnInit } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { AppConfig } from '@geonature_config/app.config';
import { ModuleConfig } from '../module.config';
import { Router } from "@angular/router";


@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  public IMPORT_CONFIG = ModuleConfig;

  constructor(
    private _commonService: CommonService,
    private _router: Router,
  ) {}


  ngOnInit() {
  }

  onProcess() {
    this._router.navigate(["import/process"]);
  }

  
}

