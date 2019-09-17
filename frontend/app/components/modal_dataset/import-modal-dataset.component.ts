import { Component, OnInit, OnDestroy } from '@angular/core';
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { ToastrService } from 'ngx-toastr';
import { Router } from '@angular/router';

import { DataService } from '../../services/data.service';
import { ModuleConfig } from '../../module.config';

@Component({
	selector: 'import-modal-dataset',
	templateUrl: 'import-modal-dataset.component.html',
	styleUrls: [ './import-modal-dataset.component.scss' ]
})
export class ImportModalDatasetComponent implements OnInit, OnDestroy {
	public selectDatasetForm: FormGroup;
	public userDatasetsResponse: any; // server response for getUserDatasets
	public datasetResponse: JSON; // server response for postDatasetResponse (= post the dataset name from the 'selectDatasetForm' form)
	public isUserDatasetError: Boolean = false; // true if user does not have any declared dataset
	public datasetError: string;
	private modalRef: NgbModalRef;

	constructor(
		private modalService: NgbModal,
		private _fb: FormBuilder,
		public _ds: DataService,
		private toastr: ToastrService,
		private _router: Router //private _idImport: importIdStorage
	) {
		this.selectDatasetForm = this._fb.group({
			dataset: [ '', Validators.required ]
		});
	}

	ngOnInit() {
		this.getUserDataset();
	}

	onOpenModal(content) {
		this.modalRef = this.modalService.open(content, {
			size: 'lg'
		});
	}

	closeModal() {
		if (this.modalRef) this.modalRef.close();
	}

	onSubmit() {
		this._router.navigate([ `${ModuleConfig.MODULE_URL}/process` ], {
			queryParams: { datasetId: this.selectDatasetForm.value.dataset }
		});
		this.closeModal();
	}

	getUserDataset() {
		// get list of all declared dataset of the user
		this._ds.getUserDatasets().subscribe(
			(result) => {
				this.userDatasetsResponse = result;
			},
			(err) => {
				if (err.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					// show error message if user does not have any declared dataset
					if (err.status == 400) {
						this.isUserDatasetError = true;
						this.datasetError = err.error;
					} else {
						// show error message if other server error
						this.toastr.error(err.error);
					}
				}
			}
		);
	}

	ngOnDestroy(): void {
		this.closeModal();
	}
}
