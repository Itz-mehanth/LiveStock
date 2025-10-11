import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';

interface StockData {
  window_end: string;
  avg_price: number;
  symbol?: string;
  company?: string;
}

@Injectable({ providedIn: 'root' })
export class DataService {
  private apiUrl = 'http://localhost:5000/api/stock_data';

  constructor(private http: HttpClient) {}

  getProcessedData(): Observable<StockData[]> {
    return this.http.get<StockData[]>(this.apiUrl).pipe(
      retry(2),
      catchError(this.handleError)
    );
  }

  getFutureTrends(symbol: string, horizon: string): Observable<any> {
  const payload = { symbol, horizon };
  return this.http.post<any>('http://localhost:5000/api/future_trends', payload)
    .pipe(
      retry(2),
      catchError(this.handleError)
    );
}


  private handleError(error: HttpErrorResponse): Observable<never> {
    let errorMessage = 'An error occurred';
    
    if (error.error instanceof ErrorEvent) {
      errorMessage = `Client Error: ${error.error.message}`;
    } else {
      errorMessage = `Server Error: ${error.status} - ${error.message}`;
    }
    
    return throwError(() => new Error(errorMessage));
  }
}