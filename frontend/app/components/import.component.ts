import { Component, OnInit } from '@angular/core';
import { CommonService } from '@geonature_common/service/common.service'; 
import { AppConfig } from '@geonature_config/app.config';
import { ToastrService } from 'ngx-toastr';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';

import { ImportModalDatasetComponent } from './import-modal-dataset.component';
import { DataService } from '../services/data.service';

@Component({
  selector: 'pnx-import',
  styleUrls: ['import.component.scss'],
  templateUrl: 'import.component.html'
})


export class ImportComponent implements OnInit {

  public importListResponse: JSON;
  public initializeResponse: JSON;

  constructor(
    private _commonService: CommonService,
    public _ds: DataService,
    private toastr: ToastrService,
    public ngbModal: NgbModal
  ) {}


  ngOnInit() {
    this.onImportList();
  }

  onProcess() {
    this._ds.initializeProcess().subscribe(
      res => {
        this.initializeResponse = res as JSON;
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
        console.log(this.initializeResponse);
        this.openDatasetModal();
      }
    );

  }

  onImportList() {
    this._ds.getImportList().subscribe(
      res => {
        this.importListResponse = res as JSON;
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
        console.log(this.importListResponse);
      }
    );
  }

  openDatasetModal() {
    const modalRef = this.ngbModal.open(ImportModalDatasetComponent, {
      centered: true, 
      size: "lg", 
      backdrop: 'static', 
      windowClass: 'dark-modal'
    });
  }
  
}

