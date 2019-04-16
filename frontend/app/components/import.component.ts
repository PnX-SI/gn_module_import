import { Component, OnInit } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { AppConfig } from '@geonature_config/app.config';
import { ModuleConfig } from '../module.config';
import { Router } from "@angular/router";
import { DataService } from '../services/data.service';
import { ToastrService } from 'ngx-toastr';


@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  public IMPORT_CONFIG = ModuleConfig;
  public importListServerMsg: JSON;

  constructor(
    private _commonService: CommonService,
    private _router: Router,
    public _ds: DataService,
    private toastr: ToastrService
  ) {}


  ngOnInit() {
    this.onImportList();
  }

  onProcess() {
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}/process`]);
  }

  onImportList() {
    this._ds.getImportList().subscribe(
      res => {
        this.importListServerMsg = res as JSON;
    },
      error => {
        if (error.statusText === 'Unknown Error') {
          // show error message if no connexion
          this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
        } else {
          // show error message if other server error
          this.toastr.error(error.error);
        }
    },
      () => {
        console.log(this.importListServerMsg);
      }
    );
  }
  
}

