import { Component, OnInit, Input } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService } from '../steps.service';

@Component({
	selector: 'upload-file-step',
	styleUrls: [ 'upload-file-step.component.scss' ],
	templateUrl: 'upload-file-step.component.html'
})
export class UploadFileStepComponent implements OnInit {
	public fileName: string;
	public isUploading: boolean = false;
	public uploadForm: FormGroup;
	public uploadFileErrors: any;
	public NextBtnDisabled: boolean = true;
	public importConfig = ModuleConfig;

	@Input() importId: number;

	constructor(
		private _activatedRoute: ActivatedRoute,
		private _ds: DataService,
		private toastr: ToastrService,
		private _fb: FormBuilder,
		private stepService: StepsService
	) {}

	ngOnInit() {
		this.uploadForm = this._fb.group({
			file: [ null, Validators.required ],
			encodage: [ null, Validators.required ],
			srid: [ null, Validators.required ],
			separator: [ null, Validators.required ]
		});
		this.Formlistener();
	}

	onFileSelected(event: any) {
		this.uploadForm.patchValue({
			file: <File>event.target.files[0]
		});
		this.fileName = this.uploadForm.get('file').value.name;
	}

	onUpload(formValues: any) {
		this.isUploading = true;

		this._ds
			.postUserFile(formValues, this._activatedRoute.snapshot.queryParams['datasetId'], this.importId)
			.subscribe(
				(res) => {
					this.importId = res.importId;
					res.srid = formValues.srid;
					this.stepService.nextStep(this.uploadForm, 'one', res);
					this.isUploading = false;
				},
				(error) => {
					this.isUploading = false;
					if (error.statusText === 'Unknown Error') {
						this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
					} else {
						if (error.status == 500) {
							this.toastr.error(error.error);
						}
						if (error.status == 400) {
							this.uploadFileErrors = error.error;
						}
						if (error.status == 403) {
							this.toastr.error(error.error);
						}
					}
				}
			);
	}

	Formlistener() {
		this.uploadForm.valueChanges.subscribe(() => {
			if (
				this.uploadForm.get('file').valid &&
				this.uploadForm.get('encodage').valid &&
				this.uploadForm.get('srid').valid &&
				this.uploadForm.get('separator').valid
			)
				this.NextBtnDisabled = false;
		});
	}
}
