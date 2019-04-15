import { Component, OnInit } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { NgbModal } from '@ng-bootstrap/ng-bootstrap'; 
import { AppConfig } from '@geonature_config/app.config';
import { ModuleConfig } from '../module.config';


@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  public IMPORT_CONFIG = ModuleConfig;

  constructor(
    private _commonService: CommonService
  ) {}


  ngOnInit() {
  }

  
}

